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
    base_dir = Directory(
        exists=True, mandatory=True,
        desc=('Path to the root of the BIDS project.'))
    default_duration = traits.Float(
        mandatory=False,
        desc=("Duration to assign when duration is missing."))
    time_repetition = traits.Float(
        mandatory=True, desc=("Sampling rate to output event files."))
    files_directory = Directory(exists=True, mandatory=False, xor=['filters'],
        desc=('Directory containing event files to use, '
              'instead of those in BIDS directory'))
    filters = traits.Dict(
        mandatory=False, xor=['event_files_directory'],
        desc=("Additional entities to filter runs on."))
    transformation_spec = traits.Either([traits.List, traits.File(exists=True)],
        mandatory=False,
        desc=("Path to JSON specification of the transformations to perform."))
    columns = traits.List(
        mandatory=False, desc=("Columns to keep on write out.")
    )
    header = traits.Bool(
        mandtory=False, desc=("Keep file headers on write out.")
    )


class TransformEventsOutputSpec(TraitedSpec):
    event_files_dir = Directory(exists=True, mandatory=True,
        desc=("Folder containing transformed event files"))


class TransformEvents(BaseInterface):

    input_spec = TransformEventsInputSpec
    output_spec = TransformEventsOutputSpec

    def _get_collection(self):
        """ Read in events """
        self.out_kwargs = {}
        for arg in ['columns', 'header']:
            attr = getattr(self.inputs, arg)
            if isdefined(attr):
                self.out_kwargs[arg] = attr

        files_dir = self.inputs.files_directory \
            if isdefined(self.inputs.files_directory) else None
        filters = self.inputs.filters\
            if isdefined(self.inputs.filters) else {}
        default_duration = self.inputs.default_duration \
                           if isdefined(self.inputs.default_duration) else None

        event_collection = BIDSEventCollection(
            base_dir=self.inputs.base_dir,
            default_duration=default_duration)
        event_collection.read(files_dir, **filters)
        return event_collection

    def _transform_events(self):
        """ Apply transformations """

        self._collection = self._get_collection()

        if isdefined(self.inputs.transformation_spec):
            self._collection.apply_from_json(self.inputs.transformation_spec)


    def _run_interface(self, runtime):
        if not have_pandas:
            raise ImportError("The Transformer interface requires pandas. "
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
