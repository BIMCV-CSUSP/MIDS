import shutil
from shutil import copyfile

import nibabel as nib
from xnat2mids.conversion.dicom_converters import dicom2niix

body_part_bids = ['head','brain']

class ProceduresMR:
    def __init__(self):
        self.reset_indexes()

    def reset_indexes(self):
        """Reset all values in dictionary of the atribute "dict_indexes"."""

        self.run_dict = {}


    def control_sequences(
        self, folder_nifti, mids_session_path, session_name,  protocol, acq, dir_, folder_BIDS, body_part
    ):
        
        folder_image_mids = mids_session_path.joinpath(
             "" if body_part.lower() in body_part_bids else "mim-mr",
            folder_BIDS
        )
        
        #folder_image_mids.mkdir(parents=True, exist_ok=True)



        self.control_image(folder_nifti, folder_image_mids, session_name, protocol, acq, dir_, body_part)

    def control_image(self, nifti_path, folder_image_mids, session_name, protocol, acq, dir_, body_part):

        """

        """


        # Search all nifti files in the old folder and sort them
        nifti_files = sorted([i for i in nifti_path.glob("*.nii.gz")])

        len_nifti_files = len(nifti_files)
        if len_nifti_files == 0: return
        folder_image_mids.mkdir(parents=True, exist_ok=True)
        # This is a counter for several nifties in one adquisition

        protocol_label = f'{protocol}'
        acq_label = f'{acq}' if acq else ''
        bp_label = f"{body_part}"
        vp_label = [
            (
                f"{self.get_plane_nib(nifti_file)}"
                if body_part.lower() in body_part_bids
                else f"{self.get_plane_nib(nifti_file)}"
            )
            for nifti_file in nifti_files
        ]
        key = str([session_name, acq_label, dir_, bp_label, vp_label, protocol_label])
        value = self.run_dict.get(key, {"runs":[], "folder_mids": None})
        value['runs'].append(nifti_files)
        value['folder_mids'] = folder_image_mids
        self.run_dict[key] = value

    def copy_sessions(self, subject_name):
        for key_str, runs in self.run_dict.items():
            keys = eval(key_str)
            print("-"*79)
            print(keys, runs)
            print("-" * 79)

            activate_run = True if len(runs["runs"]) > 1 else False
            for num_run, run in enumerate(runs["runs"],1):

                activate_nifti_parted = True if len(run) > 1 else False
                for num_part, partition in enumerate(run,1):

                    dest_file_name = self.calculate_name(
                        subject_name, keys, num_run, num_part, activate_run, activate_nifti_parted
                    )

                    print("origen:", partition)
                    print("destino:", runs["folder_mids"].joinpath(str(dest_file_name) + "".join(partition.suffixes)))
                    shutil.copyfile(partition, runs["folder_mids"].joinpath(str(dest_file_name) + "".join(partition.suffixes)))
                other_files = [f for f in partition.parent.iterdir() if "".join(partition.suffixes) not in str(f)]
                for other_file in other_files:
                    print("origen:", other_file)
                    print("destino:", runs["folder_mids"].joinpath(str(dest_file_name) + "".join(other_file.suffixes)))
                    shutil.copyfile(str(other_file), runs["folder_mids"].joinpath(str(dest_file_name) + "".join(other_file.suffixes)))

    def get_plane_nib(self, nifti):
        """
            Calculate the type of plane with the tag image orientation patient
            in dicom metadata.
        """

        img = nib.load(nifti)
        plane = nib.aff2axcodes(img.affine)[2]
        return "ax" if plane in ["S", "I"] else "sag" if plane in ["R", "L"] else "cor"

    def calculate_name(self, subject_name, keys, num_run, num_part, activate_run, activate_acq_partioned):
        print(num_part, activate_acq_partioned)
        acq = f"{keys[1] if keys[1] else ''}"
        chunk = f"{num_part if activate_acq_partioned else ''}"
        print(f"{keys=}")
        return "_".join([
            part for part in [
                subject_name,
                keys[0],
                f'acq-{acq}' if acq else '',
                f'dir-{keys[2]}' if keys[2] else '',
                f'run-{num_run}' if activate_run else '',
                (f'chunk-{chunk}') if activate_acq_partioned else '',
                '' if keys[3].lower() in body_part_bids else f'bp-{keys[3].lower()}',
                f'desc-{keys[4][num_part-1]}' if keys[3].lower() in body_part_bids else f'vp-{keys[4][num_part-1]}',
                keys[5]
            ] if part != ''
        ])

