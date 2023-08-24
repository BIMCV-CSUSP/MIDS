import subprocess
import SimpleITK as sitk
import pydicom
import json
import pydicom
import shutil
import platform
sistema = platform.system()
# def sitk_dicom2mifti(dicom_path):
#     reader = sitk.ImageSeriesReader()
#     dicom_names = reader.GetGDCMSeriesFileNames(dicom_path.parent)
#     reader.SetFileNames(dicom_names)
#     image = reader.Execute()
#
#     # Added a call to PermuteAxes to change the axes of the data
#     image = sitk.PermuteAxes(image, [2, 1, 0])
#
#     sitk.WriteImage(image, 'nifti.nii.gz')
#
# def dicom2nii(dicom_path):
#     #settings.disable_validate_slice_increment()
#     print(dicom_path.parent)
#     nifti_path = dicom_path.parent.parent.parent.joinpath("LOCAL_NIFTI", "files")
#     nifti_path.mkdir(parents=True, exist_ok=True)
#     #dicom2nifti.convert_directory(dicom_path.parent, nifti_path,  compression=True, reorient_nifti=True)
#     array_nifty = dicom2nifti.convert_dicom.dicom_series_to_nifti(str(dicom_path.parent),str(nifti_path), reorient_nifti=True)
#     #save_dicom_metadata(
#     #    dicom_path, nifti_path.parent.joinpath(nifti_path.name.split(".")[0] + ".json")
#     #)
#     return array_nifty


def dicom2niix(folder_json, str_options):
    folder_nifti = folder_json.parent.parent.joinpath("LOCAL_NIFTI", "files")
    folder_nifti.mkdir(parents=True, exist_ok=True)
    print(f"dcm2niix {str_options} -o {folder_nifti} {folder_json}")
    if sistema == "Linux":
        subprocess.call(
            f"dcm2niix {str_options} -o {folder_nifti} {folder_json}",
            shell=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.STDOUT
        )
    else:
        print(f"dcm2niix.exe {str_options} -o {folder_nifti} {folder_json}")
        subprocess.call(
            f"dcm2niix.exe {str_options} -o {folder_nifti} {folder_json}",
            shell=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.STDOUT
        )
    if len(list(folder_nifti.iterdir())) == 0:
        # folder_nifti.parent.unlink(missing_ok=True)
        return folder_nifti

    add_dicom_metadata(folder_json, folder_nifti)
    return folder_nifti


def dicom2png(folder_json, str_options):
    # shutil.move(
    #         str(folder_json.parent.parent.joinpath("LOCAL_NIFTI")),
    #         str(folder_json.parent.parent.joinpath("LOCAL_PNG"))
    # )
    if folder_json.parent.parent.joinpath("LOCAL_NIFTI").exists():
        shutil.rmtree(folder_json.parent.parent.joinpath("LOCAL_NIFTI"))
    dcm_files = list(folder_json.rglob("*dcm"))
    for dcm_file in dcm_files:
        
        
        folder_png = folder_json.parent.parent.joinpath("LOCAL_PNG","files", dcm_file.stem+".png")
        folder_png.parent.mkdir(parents=True, exist_ok=True)
        sitk_img = sitk.ReadImage(dcm_file)
        sitk.WriteImage(sitk_img, folder_png)
    if sistema == "Linux":
        subprocess.call(
            f"dcm2niix {str_options} -b o -o {folder_png.parent} {dcm_files[0].parent}",
            shell=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.STDOUT
        )
    else:
        subprocess.call(
        f"dcm2niix.exe {str_options} -b o -o {folder_png.parent} {dcm_files[0].parent}",
        shell=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.STDOUT
    )
    add_dicom_metadata(dcm_files[0].parent, folder_png.parent)
    return folder_png.parent


def add_dicom_metadata(
        folder_json, folder_nifti, list_tags = [(0x0008,0x1030), (0x0008,0x0060), (0x0020,0x0020), (0x0020,0x0060), (0x0020,0x1002), (0x0028,0x0002), (0x0028,0x0004), (0x0028, 0x0010), (0x0028, 0x0011), (0x0028, 0x1050), (0x0028, 0x1051), (0x0018,0x5100), (0x0018,0x1040), (0x0018,0x0091), (0x0018,0x0025),(0x0018,0x1060),(0x0018,0x1315),(0x0028,0x0004) ]
):
    json_filepath = list(folder_nifti.glob("*.json"))[0]
    with json_filepath.open("r") as file_json:
        dict_json = json.loads(file_json.read())
    dicom_dir = pydicom.filereader.dcmread(str(list(folder_json.glob("*.dcm"))[0]), stop_before_pixels=True)
    extract_values = {
        dicom_dir.get(key).name.replace(" ", ""):(
        str(dicom_dir.get(key).value) if not dicom_dir.get(key).is_empty else "NaN"
        )
        for key in list_tags if dicom_dir.get(key)
    }
    actualized_dict_json = dict(dict_json, **extract_values)
    string_json = json.dumps(actualized_dict_json, default=lambda o: o.__dict__,
                             sort_keys=True)
    with json_filepath.open('w') as dicom_file:
        dicom_file.write(string_json)
