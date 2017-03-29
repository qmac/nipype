from ..interfaces.base import (
    BaseInterfaceInputSpec, BaseInterface,
    File, TraitedSpec, traits, InputMultiPath, OutputMultiPath
)
from ..utils.misc import package_check

from os.path import split, abspath, basename

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
    graph_spec = traits.Dict(desc='a json spec for the graph')
    event_files = InputMultiPath(File(exists=True), mandatory=True,
                                 desc=('BIDS events.tsv file(s) containing onsets and durations '
                                       'and regressors for each run.'))

class PliersInterfaceOutputSpec(TraitedSpec):
    event_files = OutputMultiPath(File,
                                   desc=("Event files with pliers features added"))


class PliersInterface(BaseInterface):
    input_spec = PliersInterfaceInputSpec
    output_spec = PliersInterfaceOutputSpec

    def _run_interface(self, runtime):
        output_event_files = []
        for event_file in self.inputs.event_files:
            # Configure the stimuli
            stims = []
            event_df = pd.read_csv(event_file, sep='\t', na_values='n/a')
            for event in event_df.iterrows():
                stim = load_stims(event['stim_file'])
                stim.onset = event['onset']
                stim.duration = event['duration']
                stims.append(stim)

            # Construct and run the graph
            graph = Graph(self.inputs.graph_spec)
            results = graph.run(stims)

            # Format and write the output
            results = to_long_format(results)
            event_df = pd.concat([event_df, results], axis=1)
            output_file = abspath(basename(event_file))
            event_df.to_csv(output_file, sep='\t')

            output_event_files.append(output_file)

        setattr(self, '_event_files', output_event_files)
        return runtime

    def _list_outputs(self):
        return {'events_file': getattr(self, '_events_file')}
