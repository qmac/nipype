from ..interfaces.base import (
    BaseInterfaceInputSpec, BaseInterface,
    File, TraitedSpec, OutputMultiPath, traits,
    Directory
)
from ..utils.misc import package_check

from os.path import abspath, basename, join

missing_packages = []
for package in ['pandas', 'bids', 'pliers']:
    try:
        package_check(package)
    except Exception as e:
        missing_packages.append(package)

if not missing_packages:
    import pandas as pd
    from pliers.graph import Graph
    from pliers.stimuli import load_stims
    from pliers.export import to_long_format
    from bids.grabbids import BIDSLayout


class PliersInterfaceInputSpec(BaseInterfaceInputSpec):
    graph_spec = File(exists=True, mandatory=True,
                                  desc=("JSON specification of the features to extract."))
    subset = traits.Dict(mandatory=False, desc=("Dictionary of entities on which to filter,"
                                                "event files."))
    bids_dir = Directory(exists=True, mandatory=True, desc=('A BIDS directory.'))

class PliersInterfaceOutputSpec(TraitedSpec):
    event_files = OutputMultiPath(File,
                                   desc=("Event files with pliers features added"))

class PliersInterface(BaseInterface):
    input_spec = PliersInterfaceInputSpec
    output_spec = PliersInterfaceOutputSpec

    def _get_events(self, bids_dir, subset):
        """ Get a subject's event files.
        Scan all the images and get the corresponding event files.
        For now assume all event filenames have entities . """
        project = BIDSLayout(bids_dir)

        images = project.get(return_type='file', modality='func',
                                  extensions='.nii.gz', type='bold', **subset)
        if not images:
            raise Exception("No functional runs matching the request subset"
                            "found in BIDS project.")

        all_events = []
        for img_f in images:
            ## HARDCODED FOR DEVELOPMENT
            entities = ['run', 'session', 'subject', 'task']
            f_ents = project.files[img_f].entities
            f_ents = {k: v for k, v in f_ents.items() if k in entities}

            image_events = project.get(
                return_type='file', extensions='.tsv', type='events', **f_ents)

            if len(image_events) > 1:
                raise Exception("Found more than one event file per image")
            else:
                all_events.append(image_events[0])

        return all_events

    def _run_interface(self, runtime):
        stimuli_folder = join(self.inputs.bids_dir, 'stimuli')
        event_files = self._get_events(self.inputs.bids_dir, self.inputs.subset)

        print(event_files)
        # For each event file
        output_event_files = []
        for evf in event_files:
            stims = []
            df = pd.read_csv(evf, sep='\t', na_values='n/a')

            # For each stimulus
            for i, event in df.iterrows():
                stim_file = event['stim_file']
                if pd.isnull(stim_file) is False:
                    stim = load_stims(join(stimuli_folder, event['stim_file']))
                    stim.onset = event['onset']
                    stim.duration = event['duration']
                    stims.append(stim)

            # Construct and run the graph
            graph = Graph(spec=self.inputs.graph_spec)
            results = graph.run(stims)

            # Format and write the output
            results = to_long_format(results)

            # Rename columns
            # Still need to get filename
            results = results.rename(columns={'value': 'extractor_value',
                                              'feature': 'extractor_feature'})

            # Create and write files
            event_df = pd.concat([df, results], axis=0)

            ### Maybe re-order
            output_file = abspath(basename(evf))
            event_df.to_csv(output_file, sep='\t', index=False)

            output_event_files.append(output_file)

        setattr(self, '_event_files', output_event_files)
        return runtime

    def _list_outputs(self):
        return {'event_files': getattr(self, '_event_files')}
