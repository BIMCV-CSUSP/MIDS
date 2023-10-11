import pandas
import numpy
import json
import shutil
from datetime import datetime
from xnat2mids.procedures import Procedures
from pathlib import Path

view_position_regex = r"pa|ap|lat"
class RadiologyProcedure(Procedures):
    def __init__(self):
        self.view_positions_2d = ['pa', 'lat', 'ap', None]
        self.view_positions_3d = ['ax', 'sag', 'cor', None]
        self.reset_indexes()

    # def reset_indexes(self):
    #     self.dict_indexes = {
    #         "cr": {k: 1 for k in self.view_positions_2d},
    #         "dx": {k: 1 for k in self.view_positions_2d},
    #         "ct": {k: 1 for k in self.view_positions_3d},
    #         # not defined yet
    #         "bmd": {None: 1},
    #         "xa": {None: 1},
    #         "io": {None: 1},
    #         "mg": {None: 1},
    #         "vf": {None: 1},
    #         "rf": {None: 1},
    #         "rtimage": {None: 1},
    #         "rtplan": {None: 1},
    #         "rtrecord": {None: 1},
    #         "rtdose": {None: 1},
    #         "df": {None: 1},
    #         "rg": {None: 1},
    #         "ds": {None: 1},
    #     }
    # def __init__(self):
    #         self.reset_indexes()

    def reset_indexes(self):
        self.run_dict = {}

    def control_image(self, folder_conversion, mids_session_path, dict_json, session_name, modality, acquisition_date_time):
        png_files = sorted([i for i in folder_conversion.glob("*.png")])
        #nifti_files = sorted([i for i in folder_conversion.glob("*.nii*")])
        mim = modality if body_part.lower in[ "head", "brain"] else "mim-rx"
        laterality = dict_json.get("Laterality")
        view_position = dict_json.get("Laterality")
        len_files = len(png_files)
        if not len_files: return

        key = json.dumps([session_name, rec, laterality, protocol])
        value = self.run_dict.get(key, [])
        value.append({
            "run":png_files, 
            "adquisition_time":datetime.fromisoformat(acquisition_date_time), 
            "folder_mids": mids_session_path.joinpath(mim)})
        self.run_dict[key]=value
        

    def copy_sessions(self, subject_name):
        for key, runs_list in self.run_dict.items():
            df_aux = pandas.DataFrame.from_dict(runs_list)
            df_aux.sort_values(by="adquisition_time", inplace = True)
            df_aux.index = numpy.arange(1, len(df_aux) + 1)
            print(len(df_aux))
            activate_run = True if len(df_aux) > 1 else False
            print(f"{activate_run}")
            for index, row in df_aux.iterrows():
                activate_acq_partioned = True if len(row['run']) > 1 else False
                for acq, file_ in enumerate(sorted(row['run'])):
                
                    dest_file_name = self.calculate_name(
                        subject_name=subject_name, 
                        key=key,
                        num_run=index, 
                        num_part=acq, 
                        activate_run=activate_run, 
                        activate_acq_partioned=activate_acq_partioned
                    )
                    print("-"*79)
                    print(row["folder_mids"])
                    print("-"*79)
                    print("origen:", file_)
                    print("destino:", row["folder_mids"].joinpath(str(dest_file_name) + "".join(file_.suffix)))
                    
                    row["folder_mids"].mkdir(parents=True, exist_ok=True)
                    shutil.copyfile(file_, row["folder_mids"].joinpath(str(dest_file_name) + "".join(file_.suffix)))
                other_files = [f for f in file_.parent.iterdir() if file_.suffix not in str(f) and not f.is_dir()]
                for other_file in other_files:
                    print("origen:", other_file)
                    print("destino:", row["folder_mids"].joinpath(str(dest_file_name) + "".join(other_file.suffixes)))
                    shutil.copyfile(str(other_file), row["folder_mids"].joinpath(str(dest_file_name) + "".join(other_file.suffixes)))

    def calculate_name(self, subject_name, key, num_run, num_part, activate_run, activate_acq_partioned):
        key_list = json.loads(key)
        print(key_list)
        # print(num_part, activate_acq_partioned)
        
        rec = f"{key_list[1] if key_list[1] else ''}"
        chunk = f"{num_part+1 if activate_acq_partioned else ''}"
        run = f"{num_run if activate_run else ''}"
        print(f"{run}")
        # print(f"{key=}")
        return "_".join([
            part for part in [
                subject_name,
                key_list[0],
                f"acq-{key_list[2]}" if key_list[2] else "",
                f"rec-{rec}",
                f'run-{run}',
                (f'chunk-{chunk}') if activate_acq_partioned else '',
                key_list[3]
            ] if part.split('-')[-1] != ''
        ])
    

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
            copyfile(nifti_file, str(new_path_mids.joinpath(nii_name)))

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


   