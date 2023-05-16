import re

from collections import defaultdict
from datetime import datetime

import pandas

from xnat2mids.conversion.io_json import load_json
from xnat2mids.procedures.magnetic_resonance_procedures import ProceduresMR
from xnat2mids.procedures.light_procedures import LightProcedure
from xnat2mids.protocols.scans_tagger import Tagger
from xnat2mids.conversion.dicom_converters import dicom2niix
from xnat2mids.conversion.dicom_converters import dicom2png
from tqdm import tqdm


adquisition_date_pattern = r"(?P<year>\d+)-(?P<month>\d+)-(?P<day>\d+)T(?P<hour>\d+):(?P<minutes>\d+):(?P<seconds>\d+).(?P<ms>\d+)"
subses_pattern = r"[A-z]+(?P<prefix_sub>\d*)?(_S)(?P<suffix_sub>\d+)/[A-z]+\-?[A-z]*(?P<prefix_ses>\d*)?(_E)(?P<suffix_ses>\d+)"
aquisition_date_pattern_comp = re.compile(adquisition_date_pattern)
dict_keys = {
    'Modality': '00080060',
    'SeriesDescription': '0008103E',
    'ProtocolName': '00181030',
    'ComplexImage Component Attribute': '00089208',
    "ImageType" :'00080008',
    #"difusion Directionality": ''
}

dict_mr_keys = {
    'Manufacturer': '00080070',
    'ScanningSequence': '00180020',
    'SequenceVariant': '00180021',
    'ScanOptions': '00180022',
    'AngioFlag': '00180025',
    'MagneticFieldStrength': '00180087',
    'RepetitionTime': '00180080',
    'InversionTime': '00180082',
    'FlipAngle': '00181314',
    'EchoTime': '00180081',
    'SliceThickness': '00180050',
}

BIOFACE_PROTOCOL_NAMES = [
    '3D-T2-FLAIR SAG',
    '3D-T2-FLAIR SAG NUEVO-1',
    'AAhead_scout',
    'ADVANCED_ASL',
    'AXIAL T2 TSE FS',
    'AX_T2_STAR',
    'DTIep2d_diff_mddw_48dir_p3_AP', #
    'DTIep2d_diff_mddw_4b0_PA', #
    'EPAD-3D-SWI',
    'EPAD-B0-RevPE', # PA
    'EPAD-SE-fMRI',
    'EPAD-SE-fMRI-RevPE',
    'EPAD-SingleShell-DTI48', # AP
    'EPAD-rsfMRI (Eyes Open)',
    'MPRAGE_GRAPPA2', # T1 mprage
    'asl_3d_tra_iso_3.0_highres',
    'pd+t2_tse_tra_p2_3mm',
    't1_mprage_sag_p2_iso', # t1
    't2_space_dark-fluid_sag_p2_iso', # flair
    't2_swi_tra_p2_384_2mm'
]

BIOFACE_PROTOCOL_NAMES_DESCARTED = [
    #'DTIep2d_diff_mddw_48dir_p3_AP',
    #'DTIep2d_diff_mddw_4b0_PA',
    'EPAD-B0-RevPE',
    'EPAD-SingleShell-DTI48',
    'EPAD-3D-SWI',
    'EPAD-SE-fMRI',
    'EPAD-rsfMRI (Eyes Open)',
    'EPAD-SE-fMRI-RevPE',
    'AAhead_scout',
    'ADVANCED_ASL',
    'MPRAGE_GRAPPA2',
    '3D-T2-FLAIR SAG',
    '3D-T2-FLAIR SAG NUEVO-1'

]
LUMBAR_PROTOCOLS_ACEPTED = {
    't2_tse_sag_384': 556,
    't2_tse_tra_384': 534,
    't1_tse_sag_320': 523,
    't1_tse_tra': 518,
}

options_dcm2niix = "-w 0 -i n -m y -ba n -f %x_%s -z y"

def create_directory_mids_v1(xnat_data_path, mids_data_path, body_part):
    print(xnat_data_path)
    print(mids_data_path)
    procedure_class_mr = ProceduresMR()
    procedure_class_light = LightProcedure()
    for subject_xnat_path in tqdm(xnat_data_path.glob('*/')):
        if "_S" not in subject_xnat_path.name:continue
        num_sessions = len(list(subject_xnat_path.glob('*/')))
        procedure_class_mr.reset_indexes()
        procedure_class_light.reset_indexes()
        for sessions_xnat_path in subject_xnat_path.glob('*/'):
            if "_E" not in sessions_xnat_path.name: continue
            

            
            findings = re.search(subses_pattern, str(sessions_xnat_path), re.X)
            #print('subject,', findings.group('prefix_sub'), findings.group('suffix_sub'))
            #print('session,', findings.group('prefix_ses'), findings.group('suffix_ses'))
            subject_name = f"sub-{findings.group('prefix_sub')}S{findings.group('suffix_sub')}"
            session_name = f"ses-{findings.group('prefix_ses')}S{findings.group('suffix_ses')}"

            mids_session_path = mids_data_path.joinpath(subject_name, session_name)
            xml_session_rois = list(sessions_xnat_path.rglob('*.xml'))
            #print(f"1: {mids_session_path=}")
            tagger = Tagger()
            tagger.load_table_protocol(
                './xnat2mids/protocols/protocol_RM_brain.tsv'
            )
            if not sessions_xnat_path.joinpath("scans").exists(): continue
            for scans_path in sessions_xnat_path.joinpath("scans").iterdir():
                #if "LOCAL_" not in str(scans_path):
                    
                    num_jsons = len(list(scans_path.joinpath("resources").rglob("*.dcm"))) #
                    
                    
                    if num_jsons ==0:
                        continue
                    if num_jsons>0:
                        path_dicoms= list(scans_path.joinpath("resources").rglob("*.dcm"))[0].parent
                        folder_conversion = dicom2niix(path_dicoms, options_dcm2niix) #.joinpath("resources")
                    else:
                        continue
                        # path_dicoms= list(scans_path.joinpath("resources").rglob("*.dcm"))[0].parent
                        # folder_conversion = dicom2png(path_dicoms, options_dcm2niix) #.joinpath("resources")
                    
                    print("---------", len(list(folder_conversion.iterdir())))
                    if len(list(folder_conversion.iterdir())) == 0: continue
                    
                    dict_json = load_json(folder_conversion.joinpath(list(folder_conversion.glob("*.json"))[0]))


                    modality = dict_json.get("Modality", "n/a")
                    study_description = dict_json.get("SeriesDescription", "n/a")
                    Protocol_name = dict_json.get("ProtocolName", "n/a")
                    image_type = dict_json.get("ImageType", "n/a")
                    acquisition_date_time = dict_json.get("AcquisitionDateTime", "")
                    body_part = dict_json.get("BodyPartExamined", body_part)
                    acquisition_date_time_check = aquisition_date_pattern_comp.search(acquisition_date_time)
                    time_values = list(int (x) for x in acquisition_date_time_check.groups())
                    acquisition_date_time_correct = f"\
{time_values[0]:04d}-\
{time_values[1]:02d}-\
{time_values[2]:02d}T\
{time_values[3]:02d}:\
{time_values[4]:02d}:\
{time_values[5]:02d}.\
{time_values[6]:06d}\
"
                    # print()
                    # print(modality, study_description, ProtocolName, image_type)
                    print(f"{study_description=}")
                    print(f"{Protocol_name=}")
                    if modality == "MR":
                        # via BIDS protocols
                        #if study_description in LUMBAR_PROTOCOLS_ACEPTED:
                            
                            
                            json_adquisitions = {
                                f'{k}': dict_json.get(k, -1) for k in dict_mr_keys.keys()
                            }
                            
                            protocol, acq, task, ce, rec, dir_, part, folder_BIDS = tagger.classification_by_min_max(json_adquisitions)
                            print(protocol, acq, task, ce, rec, dir_, part, folder_BIDS)
                            continue
                            procedure_class_mr.control_sequences(
                                folder_conversion, mids_session_path, session_name, protocol, acq, dir_, folder_BIDS, body_part
                            )
                    
                    if modality == "OP":
                        laterality = dict_json.get("Laterality")
                        acq = "" if "ORIGINAL" in image_type else "opacitysubstract"
                        # print(laterality, acq)
                        procedure_class_light.control_image(folder_conversion, mids_session_path.joinpath("mim-light"), session_name, "op", acq, laterality, acquisition_date_time_correct)
        procedure_class_mr.copy_sessions(subject_name)
        procedure_class_light.copy_sessions(subject_name)


participants_header = ['participant', 'modalities', 'body_parts', 'patient_birthday', 'age', 'gender']
participants_keys = ['Modality', 'BodyPartExamined', 'PatientBirthDate', 'PatientSex', 'AcquisitionDateTime']
session_header = ['session', 'acquisition_date_Time',]
def create_tsvs(xnat_data_path, mids_data_path, body_part_aux):
    """
        This function allows the user to create a table in format ".tsv"
        whit a information of subject
        """
    
    list_information= []
    for subject_path in mids_data_path.glob('*/'):
        if not subject_path.match("sub-*"): continue
        subject = subject_path.parts[-1]
        for session_path in subject_path.glob('*/'):
            if not session_path.match("ses-*"): continue
            session = session_path.parts[-1]
            modalities = []
            body_parts = []
            patient_birthday = None
            patient_ages = list([])
            patient_sex = None
            adquisition_date_time = None
            for json_pathfile in subject_path.glob('**/*.json'):
                json_file = load_json(json_pathfile)
                print(json_file)
                modalities.append(json_file[participants_keys[0]])
                try:
                    body_parts.append(json_file[participants_keys[1]].lower())
                except KeyError as e:
                    body_parts.append(body_part_aux.lower())
                patient_birthday = datetime.fromisoformat(json_file[participants_keys[2]])
                patient_sex = json_file[participants_keys[3]]
                adquisition_date_time = datetime.fromisoformat(json_file[participants_keys[4]].split('T')[0])
                patient_ages.append(int((adquisition_date_time - patient_birthday).days / (365.25)))
            patient_ages = sorted(list(set(patient_ages)))
            modalities = sorted(list(set(modalities)))
            body_parts = sorted(list(set(body_parts)))
        list_information.append({
            key:value
            for key, value in zip(
                participants_header,
                [subject, modalities, body_parts, str(patient_birthday.date()), patient_ages, patient_sex]
            )
        })
    print(list_information)
    pandas.DataFrame.from_dict(list_information).to_csv(
        mids_data_path.joinpath("participants.tsv"), sep="\t", index=False
    )
