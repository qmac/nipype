from __future__ import division

from ..interfaces.base import (BaseInterface, TraitedSpec, InputMultiPath,
                                    traits, File, Directory, Bunch,
                                    BaseInterfaceInputSpec,
                                    isdefined, OutputMultiPath)
from ..utils.misc import package_check
from nipype import logging
iflogger = logging.getLogger('interface')

from bids.events import BIDSEventCollection
from bids.events import BIDSTransformer

import os
import glob

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
    event_files_directory = InputMultiPath(
        File(exists=True), mandatory=False, xor=['filters'],
        desc=('Directory containing event files to use, '
              'instead of those in BIDS directory'))
    filters = traits.Dict(
        mandatory=False, xor=['event_files_directory'],
        desc=("Additional entities to filter runs on."))
    transformation_spec = File(
        exists=True, mandatory=True,
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


class TransformEventsOutputSpec(TraitedSpec):
    event_files = OutputMultiPath(
        Bunch, mandatory=True,
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
                kwargs['arg'] = attr

        files_directory = self.inputs.event_files \
            if isdefined(self.inputs.event_files_directory) else None
        filters = self.inputs.filters\
            if isdefined(self.inputs.filters) else {}

        event_collection = BIDSEventCollection(
            base_dir=self.inputs.bids_directory, **kwargs)
        event_collection.read(files_directory, **filters)

        return event_collection

    def _transform_events(self):
        """ Apply transformations """

        collection = self._get_collection()

        self._transformer = BIDSTransformer(collection)

        if isdefined(self.inputs.transformation_spec):
            self._transformer.apply_from_json(self.inputs.transformation_spec)


    def _run_interface(self, runtime):
        if not have_pandas:
            raise ImportError("The SpecifyEvents interface requires pandas. "
                              "Please make sure that pandas is installed.")
        self._transform_events()
        return runtime

    def _get_event_files(self):
        cd = os.getcwd()
        self.transformer.write(path=cd, sampling_rate=1./self.inputs.TR)
        return glob.glob(os.path.join(os.getcwd(), '*_events.tsv'))

    def _list_outputs(self):
        outputs = self._outputs().get()
        outputs['event_files'] = self._get_event_files()
        return outputs
