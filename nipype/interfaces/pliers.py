from ..interfaces.base import (
    BaseInterfaceInputSpec, BaseInterface,
    File, TraitedSpec, InputMultiPath, OutputMultiPath, traits,
    Directory
)
from ..utils.misc import package_check

from os.path import abspath, basename, join

for package in ['pandas', 'pliers']:
    try:
        package_check(package)
    except Exception as e:
        raise ImportError(
            "The package {} is required, but not installed".format(package))

from pliers.graph import Graph
from pliers.stimuli import load_stims
from pliers.export import to_long_format
import pandas as pd


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

    def _get_events(bids_dir, subset):
        """ Get a subject's event files """
        from bids.grabbids import BIDSLayout
        layout = BIDSLayout(bids_dir)
        events = layout.get(type='events', return_type='file', **subset)
        return events

    def _run_interface(self, runtime):
        stimuli_folder = join(self.inputs.bids_dir, 'stimuli')

        # For each event file
        output_event_files = []
        for event_file in self.inputs.event_files:
            stims = []
            event_df = pd.read_csv(event_file, sep='\t', na_values='n/a')
            # For each stimulus
            for i, event in event_df.iterrows():
                stim_file = event['stim_file']
                if pd.isnull(stim_file) is False:
                    stim = load_stims(join(stimuli_folder, event['stim_file']))
                    stim.onset = event['onset']
                    stim.duration = event['duration']
                    stim.name = event['stim_file']
                    stims.append(stim)

            # Construct and run the graph
            graph = Graph(spec=self.inputs.graph_spec)
            results = graph.run(stims)

            # Format and write the output
            results = to_long_format(results)

            # Rename "stim" to "stim_file"
            results = results.rename(columns={'stim': 'stim_file',
                                              'value': 'extractor_value',
                                              'feature': 'extractor_feature'})

            # Create and write files
            event_df = pd.concat([event_df, results], axis=0)

            ### Maybe re-order
            output_file = abspath(basename(event_file))
            event_df.to_csv(output_file, sep='\t', index=False)

            output_event_files.append(output_file)

        setattr(self, '_event_files', output_event_files)
        return runtime

    def _list_outputs(self):
        return {'event_files': getattr(self, '_event_files')}
