import subprocess
import SimpleITK as sitk
import pydicom
import json
import pydicom
import shutil
import platform
import dicom2nifti
from xnat2mids.conversion.io_json import load_json, save_json
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

def dicom2nifti(folder_json):
    folder_nifti = folder_json.parent.parent.joinpath("LOCAL_NIFTI", "files", "nifti_image.nii.gz")
    folder_nifti.mkdir(parents=True, exist_ok=True)
    dicom2nifti.convert_directory(dicom_path, folder_nifti)
    shutil.copy2(str(folder_json.joinpath("bids.json"), str(folder_nifti.parent)))
    if folder_json.joinpath("note.txt").exist():
        shutil.copy2(str(folder_json.joinpath("note.txt"), str(folder_nifti.parent)))

def dicom2niix(folder_json, str_options):
    folder_nifti = folder_json.parent.parent.joinpath("LOCAL_NIFTI", "files")
    folder_nifti.mkdir(parents=True, exist_ok=True)
    #print(f"dcm2niix {str_options} -o {folder_nifti} {folder_json}")
    if sistema == "Linux":
        subprocess.call(
            f"dcm2niix {str_options} -o {folder_nifti} {folder_json}",
            shell=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.STDOUT
        )
    else:
        #print(f"dcm2niix.exe {str_options} -o {folder_nifti} {folder_json}")
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


def dicom2png(folder_json):
    dcm_files = list(folder_json.rglob("*.dcm"))
    
    for dcm_file in dcm_files:
        
        
        folder_png = folder_json.parent.parent.joinpath("LOCAL_PNG","files", dcm_file.stem+".png")
        folder_png.parent.mkdir(parents=True, exist_ok=True)
        try:
            sitk_img = sitk.ReadImage(dcm_file)
        except RuntimeError:
            print(f"error to convert: {dcm_file}")
            continue
        sitk.WriteImage(sitk_img, folder_png)
    shutil.copy2(str(folder_json.joinpath("bids.json")), str(folder_png.parent))
    if folder_json.joinpath("note.txt").exists():
        shutil.copy2(str(folder_json.joinpath("note.txt")), str(folder_png.parent))
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

def dict2bids(dict_):
    dicom_dict = {}
    if len(dict_)==1:
        dict_items = list(dict_.items())[0]
        if dict_items[0].isdigit():
            
            dicom_dict[pydicom.datadict.keyword_for_tag(dict_items[0])] = dict_items[1].get("Value", [""])[0]
            return dicom_dict
        else:
            return dict_items[1]
    else:
        for key, element in dict_.items():
            
            description = pydicom.datadict.keyword_for_tag(key)
            
            elem = element.get("Value", [""]) if element.get("Value", [""]) else [""]#, type(element.get("Value", [""])[0]),type(element.get("Value", [""])[0]) is not dict )
            elem = elem[0] if len(elem)<2 else elem
            dicom_dict[description] = elem if type(elem) is not dict else dict2bids(element.get("Value", [""])[0])
            
    return dicom_dict

def generate_json_dicom(folder_json):
    json_file = load_json(folder_json.joinpath("dicom.json"))
    json_dict = dict2bids(json_file)
    save_json(json_dict, folder_json.joinpath("bids.json"))
    return json_dict