import pandas
import numpy
import json
import shutil
from datetime import datetime
from xnat2mids.procedures import Procedures
class LightProcedure(Procedures):
    def __init__(self):
            self.reset_indexes()

    def reset_indexes(self):
        self.run_dict = {}

    def control_image(self, folder_conversion, mids_session_path, session_name, protocol, rec, laterality, acquisition_date_time):
        png_files = sorted([i for i in folder_conversion.glob("*.png")])
        nifti_files = sorted([i for i in folder_conversion.glob("*.nii*")])

        len_files = len(png_files) + len(nifti_files)
        if not len_files: return

        key = json.dumps([session_name, rec, laterality, protocol])
        value = self.run_dict.get(key, [])
        value.append({
            "run":[*png_files, *nifti_files], 
            "adquisition_time":datetime.fromisoformat(acquisition_date_time), 
            "folder_mids": mids_session_path})
        self.run_dict[key]=value
        

    def copy_sessions(self, subject_name):
        for key, runs_list in self.run_dict.items():
            df_aux = pandas.DataFrame.from_dict(runs_list)
            df_aux.sort_values(by="adquisition_time", inplace = True)
            df_aux.index = numpy.arange(1, len(df_aux) + 1)
            print(len(df_aux))
            activate_run = True if len(df_aux) > 1 else False
            print(f"{activate_run=}")
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
        # print(num_part, activate_acq_partioned)
        rec = f"{key_list[1] if key_list[1] else ''}"
        chunk = f"{num_part+1 if activate_acq_partioned else ''}"
        run = f"{num_run if activate_run else ''}"
        print(f"{run=}")
        # print(f"{key=}")
        return "_".join([
            part for part in [
                subject_name,
                key_list[0],
                f"acq-{key_list[2]}",
                f"rec-{rec}",
                f'run-{run}',
                (f'chunk-{chunk}') if activate_acq_partioned else '',
                key_list[3]
            ] if part.split('-')[-1] != ''
        ])
