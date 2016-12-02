from __future__ import division

import numpy as np
from nipype.external.six import string_types
from nipype.interfaces.base import (BaseInterface, TraitedSpec, InputMultiPath,
                               traits, File, Bunch, BaseInterfaceInputSpec,
                               isdefined, OutputMultiPath)
from nipype import logging
import re
from glob import glob
from os.path import basename
import json
iflogger = logging.getLogger('interface')

from coda import EventReader, EventTransformer

have_pandas = True
try:
    import pandas as pd
except:
    have_pandas = False


class SpecifyEventsInputSpec(BaseInterfaceInputSpec):
    subject_info = InputMultiPath(Bunch, mandatory=True, xor=['subject_info',
                                                              'event_files' , 'bids_events'],
                                  desc=("Bunch or List(Bunch) subject specific condition information. "
                                        "see :ref:`SpecifyModel` or SpecifyModel.__doc__ for details"))
    event_files = InputMultiPath(traits.List(File(exists=True)), mandatory=True,
                                 xor=['subject_info', 'event_files', 'bids_events'],
                                 desc=('list of event description files in 1, 2, 3, or 4 column format '
                                       'corresponding to onsets, durations, amplitudes, and output'))
    bids_events = InputMultiPath(traits.List(File(exists=True)), mandatory=True,
                                 xor=['subject_info', 'event_files', 'bids_events'],
                                 desc=('a BIDS events.tsv file containing onsets and durations '
                                       'and regressors as columns.'))
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


class SpecifyEvents(BaseInterface):

    input_spec = SpecifyEventsInputSpec
    output_spec = SpecifyEventsOutputSpec

    def _get_event_data(self):
        if isdefined(self.inputs.subject_info):
            info = self.inputs.subject_info
            return pd.from_records(info) ## Pretty sure this doesn't work
        elif isdefined(self.inputs.event_files):
            info = self.inputs.event_files
            reader = EventReader(columns=['onset', 'duration', 'amplitude'])
            return reader.read(info[0])
        else:
            info = self.inputs.bids_events
            reader = BIDSEventReader()
            return reader.read(info[0])  

    def _transform_events(self):
        events = self._get_event_data()
        self.transformer = EventTransformer(events)
        if isdefined(self.inputs.transformations):
            transformer.apply_from_json(self.inputs.transformations)

        self.transformer.resample(self.inputs.time_repetition)

    def _run_interface(self, runtime):
        if not have_pandas:
            raise ImportError("The SpecifyEvents interface requires pandas. "
                              "Please make sure that pandas is installed.")
        self._transform_events()
        return runtime

    def _df_to_bunch(self):
        
        if not hasattr(self, 'transformer'):
            self._transform_events()

        _data = self.transformer.data

        info = Bunch(conditions=[], onsets=[], durations=[], amplitudes=[])
        cols = [c for c in _data.columns if c not in {'onset'}]
        onsets = _data['onset'].values.tolist()
        info.conditions = cols

        for col in cols:
            info.onsets.append(onsets)
            info.durations.append(self.inputs.time_repetition)
            info.amplitudes.append(_data[col].values.tolist())

        return info

    def _list_outputs(self):
        outputs = self._outputs().get()
        outputs['subject_info'] = self._df_to_bunch()
        return outputs

