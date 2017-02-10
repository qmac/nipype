from __future__ import division

from coda import FSLEventReader, EventTransformer, BIDSEventReader
from nipype.interfaces.base import (BaseInterface, TraitedSpec, InputMultiPath,
                                    traits, File, Bunch, BaseInterfaceInputSpec,
                                    isdefined, OutputMultiPath)
from nipype import logging
iflogger = logging.getLogger('interface')


have_pandas = True
try:
    import pandas as pd
except:
    have_pandas = False


class SpecifyEventsInputSpec(BaseInterfaceInputSpec):
    subject_info = InputMultiPath(Bunch, mandatory=True, xor=['subject_info',
                                                              'event_files', 'bids_events'],
                                  desc=("Bunch or List(Bunch) subject specific condition information. "
                                        "see :ref:`SpecifyModel` or SpecifyModel.__doc__ for details"))
    event_files = InputMultiPath(traits.List(File(exists=True)), mandatory=True,
                                 xor=['subject_info', 'event_files', 'bids_events'],
                                 desc=('list of event description files in 1, 2, 3, or 4 column format '
                                       'corresponding to onsets, durations, amplitudes, and output'))
    bids_events = InputMultiPath(File(exists=True), mandatory=True,
                                 xor=['subject_info', 'event_files', 'bids_events'],
                                 desc=('BIDS events.tsv file(s) containing onsets and durations '
                                       'and regressors for each run.'))
    amplitude_column = traits.String(mandatory=False, requires='bids_events',
                                     desc=("Column in events file to read amplitude from"))
    condition_column = traits.String(mandatory=False, requires='bids_events',
                                     desc=("Column in events file that codes conditions"))
    input_units = traits.Enum('secs', 'scans', mandatory=True,
                              desc=("Units of event onsets and durations (secs or scans). Output "
                                    "units are always in secs"))
    time_repetition = traits.Float(mandatory=True,
                                   desc=("Time between the start of one volume to the start of "
                                         "the next image volume."))
    transformations = traits.File(exists=True, mandatory=False,
                                  desc=("JSON specification of the transformations to perform."))


class SpecifyEventsOutputSpec(TraitedSpec):
    subject_info = OutputMultiPath(Bunch, mandatory=True,
                                   desc=("Bunch or List(Bunch) subject specific condition information. "
                                         "see :ref:`SpecifyModel` or SpecifyModel.__doc__ for details"))
    str_info = traits.String(mandatory=False)


class SpecifyEvents(BaseInterface):

    input_spec = SpecifyEventsInputSpec
    output_spec = SpecifyEventsOutputSpec

    def _get_event_data(self):
        if isdefined(self.inputs.subject_info):
            info = self.inputs.subject_info
            all_events = []

            # Process each run in subjects_info
            for run in info:
                run_dict = run.dictcopy()
                keys = run.keys()

                # Process each condition separately
                by_condition = zip(*run_dict.values())
                all_conds = []
                for condition in by_condition:
                    cond_dict = dict(zip(keys, condition))
                    if len(cond_dict['durations']) == 1:
                        cond_dict['durations'] = cond_dict['durations'][0]
                    all_conds.append(pd.DataFrame(cond_dict))
                all_conds = pd.concat(all_conds)
                all_conds.rename({'onsets': 'onset', 'amplitudes': 'amplitude',
                                  'conditions': 'condition', 'durations': 'duration'})

                all_events.append(all_conds)

            return all_events

        elif isdefined(self.inputs.event_files):
            info = self.inputs.event_files
            reader = FSLEventReader(columns=['onset', 'duration', 'amplitude'])
            return reader.read(info[0])
        else:
            info = self.inputs.bids_events

            kwargs = {}
            if isdefined(self.inputs.amplitude_column):
                kwargs['amplitude_column'] = self.inputs.amplitude_column
            if isdefined(self.inputs.condition_column):
                kwargs['condition_column'] = self.inputs.condition_column

            reader = BIDSEventReader(**kwargs)
            return [reader.read(event) for event in info]

    def _transform_events(self):
        events = self._get_event_data()
        self.data = []
        for event in events:
            transformer = EventTransformer(event)
            if isdefined(self.inputs.transformations):
                transformer.apply_from_json(self.inputs.transformations)
            transformer.resample(self.inputs.time_repetition)
            self.data.append(transformer.data)

    def _run_interface(self, runtime):
        if not have_pandas:
            raise ImportError("The SpecifyEvents interface requires pandas. "
                              "Please make sure that pandas is installed.")
        self._transform_events()
        return runtime

    def _df_to_bunch(self):

        if not hasattr(self, 'transformer'):
            self._transform_events()

        _data = self.data

        bunches = []
        for run in _data:
            info = Bunch(conditions=[], onsets=[], durations=[], amplitudes=[])
            cols = [c for c in run.columns if c not in {'onset'}]
            onsets = run['onset'].values.tolist()
            info.conditions = cols

            for col in cols:
                info.onsets.append(onsets)
                info.durations.append([self.inputs.time_repetition])
                info.amplitudes.append(run[col].values.tolist())

            bunches.append(info)

        return bunches

    def _list_outputs(self):
        outputs = self._outputs().get()
        outputs['str_info'] = str([d.to_dict() for d in self.data])
        outputs['subject_info'] = self._df_to_bunch()
        return outputs
