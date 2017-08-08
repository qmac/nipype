from __future__ import division

from ..interfaces.base import (BaseInterface, TraitedSpec,
                                    traits, Directory,
                                    BaseInterfaceInputSpec,
                                    isdefined)
from ..utils.misc import package_check
from nipype import logging
iflogger = logging.getLogger('interface')

from bids.events import BIDSEventCollection

import os

try:
    package_check('pandas')
except Exception as e:
    have_pandas = False
else:
    have_pandas = True

class TransformEventsInputSpec(BaseInterfaceInputSpec):
    bids_directory = Directory(
        exists=True, mandatory=True,
        desc=('Path to the root of the BIDS project.'))
    event_files_dir = Directory(exists=True, mandatory=False, xor=['filters'],
        desc=('Directory containing event files to use, '
              'instead of those in BIDS directory'))
    filters = traits.Dict(
        mandatory=False, xor=['event_files_directory'],
        desc=("Additional entities to filter runs on."))
    transformation_spec = traits.Either([traits.List, traits.File(exists=True)],
        mandatory=False,
        desc=("Path to JSON specification of the transformations to perform."))
    amplitude_column = traits.String(
        mandatory=False,
        desc=("Column providing the amplitudes that correspond to to the"
              "'condition' column. Ignored if condition_column is None."))
    condition_column = traits.String(
        mandatory=False,
        desc=("Column that contains the names of conditions. Defaults to"
              "'trial_type', per the BIDS specification. If None, only extra"
              "columns (beyond onset and duration) are inspected for events."))
    default_duration = traits.Float(
        mandatory=False,
        desc=("Duration to assign when duration is missing."))
    default_amplitude = traits.Float(
        mandatory=False,
        desc=("Default amplitude to assign to events in cases where an"
              "amplitude is missing."))
    time_repetition = traits.Float(
        mandatory=True, desc=("Sampling rate to output event files."))
    columns = traits.List(
        mandatory=False, desc=("Columns to keep on writeout")
    )
    header = traits.Bool(
        mandtory=False, desc=("Keep file headers on write out")
    )


class TransformEventsOutputSpec(TraitedSpec):
    event_files_dir = Directory(exists=True, mandatory=True,
        desc=("List of transformed event files"))


class TransformEvents(BaseInterface):

    input_spec = TransformEventsInputSpec
    output_spec = TransformEventsOutputSpec

    def _get_collection(self):
        """ Read in events """
        # Init kwargs
        kwargs = {}
        for arg in ['amplitude_column', 'condition_column', 'default_duration',
                    'default_amplitude']:
            attr = getattr(self.inputs, arg)
            if isdefined(attr):
                kwargs[arg] = attr

        self.out_kwargs = {}
        for arg in ['columns', 'header']:
            attr = getattr(self.inputs, arg)
            if isdefined(attr):
                self.out_kwargs[arg] = attr

        ef_dir = self.inputs.event_files_dir \
            if isdefined(self.inputs.event_files_dir) else None
        filters = self.inputs.filters\
            if isdefined(self.inputs.filters) else {}

        event_collection = BIDSEventCollection(
            base_dir=self.inputs.bids_directory, **kwargs)
        event_collection.read(ef_dir, **filters)
        return event_collection

    def _transform_events(self):
        """ Apply transformations """

        self._collection = self._get_collection()

        if isdefined(self.inputs.transformation_spec):
            self._collection.apply_from_json(self.inputs.transformation_spec)


    def _run_interface(self, runtime):
        if not have_pandas:
            raise ImportError("The SpecifyEvents interface requires pandas. "
                              "Please make sure that pandas is installed.")
        self._transform_events()
        return runtime

    def _get_event_files(self):
        path = os.path.join(os.getcwd(), 'new_events')
        if not os.path.exists(path):
            os.mkdir(path)
        self._collection.write(path=path,
                               sampling_rate=1./self.inputs.time_repetition,
                               sparse=False,
                               **self.out_kwargs)
        return path

    def _list_outputs(self):
        outputs = self._outputs().get()
        outputs['event_files_dir'] = self._get_event_files()
        return outputs
