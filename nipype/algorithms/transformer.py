from __future__ import division

from ..interfaces.base import (BaseInterface, TraitedSpec, InputMultiPath,
                                    traits, File, Directory, Bunch,
                                    BaseInterfaceInputSpec,
                                    isdefined, OutputMultiPath)
from ..utils.misc import package_check
from nipype import logging
iflogger = logging.getLogger('interface')

from bids.transformer.base import BIDSEventCollection
from bids.transformer.transform import BIDSTransformer

try:
    package_check('pandas')
except Exception as e:
    have_pandas = False
else:
    have_pandas = True

class TransformEventsInputSpec(BaseInterfaceInputSpec):
    bids_directory = Directory(exists=True, mandatory=True, desc=('BIDS directory'))
    event_files = InputMultiPath(File(exists=True), mandatory=False,
                                 desc=('Event files to use, instead of those in BIDS directory'))
    transformation_spec = File(exists=True, mandatory=False,
                                  desc=("JSON specification of the transformations to perform."))
    ### Maybe some optional flags for how to select event files, if not provided? e.g. subjects?

    ## Are the following necessary? Do we need to read form extractor_name rather than trial_type?
    amplitude_column = traits.String(mandatory=False, requires='bids_events',
                                     desc=("Column in events file to read amplitude from"))
    condition_column = traits.String(mandatory=False, requires='bids_events',
                                     desc=("Column in events file that codes conditions"))



class TransformEventsOutputSpec(TraitedSpec):
    ## Or output as event files?
    subject_info = OutputMultiPath(Bunch, mandatory=True,
                                   desc=("Bunch or List(Bunch) subject specific condition information. "
                                         "see :ref:`SpecifyModel` or SpecifyModel.__doc__ for details"))


class TransformEvents(BaseInterface):

    input_spec = TransformEventsInputSpec
    output_spec = TransformEventsOutputSpec

    def _get_collection(self):
        """ Read in events """
        if isdefined(self.inputs.event_files):
            event_files = self.inputs.event_files
        else:
            event_files = None

        kwargs = {}
        if isdefined(self.inputs.amplitude_column):
            kwargs['amplitude_column'] = self.inputs.amplitude_column
        if isdefined(self.inputs.condition_column):
            kwargs['condition_column'] = self.inputs.condition_column

        event_collection = BIDSEventCollection(
            base_dir=self.inputs.bids_directory, **kwargs)
        event_collection.read(event_files)

        return event_collection

    def _transform_events(self):
        """ Apply transformations """

        collection = self._get_collection()

        transformer = BIDSTransformer(collection)

        if isdefined(self.inputs.transformation_spec):
            transformer.apply_from_json(self.inputs.transformation_spec)

    def _run_interface(self, runtime):
        if not have_pandas:
            raise ImportError("The SpecifyEvents interface requires pandas. "
                              "Please make sure that pandas is installed.")
        self._transform_events()
        return runtime

    def _df_to_bunch(self):
        ### Do we still want to do bunches?

        ## Either way what is the output format?

        ## Split by columns and runs? So it can be used in a MapNode?
        pass

    def _list_outputs(self):
        outputs = self._outputs().get()
        outputs['subject_info'] = self._df_to_bunch()
        return outputs
