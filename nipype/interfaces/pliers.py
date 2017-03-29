from nipype.interfaces.base import (
    BaseInterfaceInputSpec, BaseInterface,
    File, TraitedSpec, traits
)
from nipype.utils.misc import package_check

import pandas as pd

have_pliers = True
try:
    package_check('pliers')
except Exception as e:
    have_pliers = False
else:
    from pliers.graph import Graph
    from pliers.stimuli import load_stims
    from pliers.export import to_long_format


class PliersInterfaceInputSpec(BaseInterfaceInputSpec):
    graph_spec = traits.Dict(desc='a json spec for the graph')
    events_file = File(exists=True, mandatory=True, desc='an events file')


class PliersInterfaceOutputSpec(TraitedSpec):
    events_file = File(exists=True, mandatory=True, desc='an events file')


class PliersInterface(BaseInterface):
    input_spec = PliersInterfaceInputSpec
    output_spec = PliersInterfaceOutputSpec

    def _run_interface(self, runtime):
        # Configure the stimuli
        stims = []
        event_df = pd.read_csv(self.inputs.events_file, na_values='n/a')
        for event in event_df.iterrows():
            stim = load_stims(event['stim_file'])
            stim.onset = event['onset']
            stim.duration = event['duration']
            stims.append(stim)

        # Construct and run the graph
        graph = Graph(self.inputs.graph_spec)
        results = graph.run(stims)
        print(results)

        # Format and write the output
        results = to_long_format(results)
        event_df = pd.concat([event_df, results], axis=1)
        event_df.to_csv(self.inputs.events_file)
        setattr(self, '_events_file', self.inputs.events_file)
        return runtime

    def _list_outputs(self):
        return {'events_file': getattr(self, '_events_file')}
