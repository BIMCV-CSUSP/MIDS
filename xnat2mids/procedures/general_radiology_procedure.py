from shutil import copyfile
from pathlib import Path
from xnat2mids.procedures import Procedures


class GeneralRadiologyProcedures(Procedures):
    def __init__(self):
        self.view_positions_2d = ['pa', 'll', 'lateral', 'ap', None]
        self.view_positions_3d = ['ax', 'sag', 'cor', None]
        self.reset_indexes()

    def reset_indexes(self):
        self.dict_indexes = {
            "cr": {k: 1 for k in self.view_positions_2d},
            "dx": {k: 1 for k in self.view_positions_2d},
            "ct": {k: 1 for k in self.view_positions_3d},
            # not defined yet
            "bmd": {None: 1},
            "xa": {None: 1},
            "io": {None: 1},
            "mg": {None: 1},
            "vf": {None: 1},
            "rf": {None: 1},
            "rtimage": {None: 1},
            "rtplan": {None: 1},
            "rtrecord": {None: 1},
            "rtdose": {None: 1},
            "df": {None: 1},
            "rg": {None: 1},
            "ds": {None: 1},
        }

    def procedure(
            self, department_id, subject_id, session_id, body_part,
            view_position, dicom_modality):
        """
            Function that copies the elements to the CR images of
            the mids.
        """
        nifti_files = sorted([str(i) for i in Path('/'.join(str(json_path).split('/')[:-4])).glob("**/*.png")])
        len_nifti_files = len(nifti_files)
        new_path_mids = self.get_mids_path(mids_path=department_id)

        os.makedirs(new_path_mids)
        for num, nifti_file in enumerate(nifti_files):
            nii_name =self.get_name(
                subject_id=subject_id,
                session_id=session_id,
                acq_index=acq_index,
                run_index=str(self.dict_indexes[scan][view_position]),
                len_nifti_files=len_nifti_files,
                body_part=body_part,
                view_position=view_position,
                scan=dicom_modality,
                ext=".nii.gz" if dicom_modality=="cr" else ".png"
            )
            # copy the nifti file in the new path
            copyfile(nifti_file, str(new_path_mids.joinpath(nii_name))

        json_name = self.get_name(
            subject_id=subject_id,
            session_id=session_id,
            acq_index=acq_index,
            run_index=str(self.dict_indexes[scan][iop]),
            len_nifti_files=len_nifti_files,
            body_part=body_part,
            view_position=view_position,
            scan="cr",
            ext=".json"
        )
            copyfile(str(dicom_json), str(new_path_mids.joinpath(json_name)))