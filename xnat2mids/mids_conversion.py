import re

from collections import defaultdict
from datetime import datetime
from pathlib import Path
import pandas
from xnat2mids.conversion.io_json import load_json
from xnat2mids.conversion.dicom_converters import generate_json_dicom
from xnat2mids.procedures.magnetic_resonance_procedures import ProceduresMR
from xnat2mids.procedures.light_procedures import LightProcedure
from xnat2mids.procedures.general_radiology_procedure import RadiologyProcedure
from xnat2mids.protocols.scans_tagger import Tagger
from xnat2mids.conversion.dicom_converters import dicom2niix
from xnat2mids.conversion.dicom_converters import dicom2png
from tqdm import tqdm
from pandas.errors import EmptyDataError
##1003-2-4T02:23:43.3245
##20231212020401.23452
adquisition_date_pattern_2 = r"(?P<fecha1>(?P<year>\d{4})-(?P<month>\d+)-(?P<day>\d+)T(?P<hour>\d+):(?P<minutes>\d+):(?P<seconds>\d+).(?P<ms>\d+))|(?P<fecha2>(?P<year2>\d{4})(?P<month2>\d{2})(?P<day2>\d{2})(?P<hour2>\d{2})(?P<minutes2>\d{2})(?P<seconds2>\d{2}).(?P<ms2>\d+))"
adquisition_date_pattern = r"(?P<year>\d+)-(?P<month>\d+)-(?P<day>\d+)T(?P<hour>\d+):(?P<minutes>\d+):(?P<seconds>\d+).(?P<ms>\d+)"
subses_pattern = r"[A-z]+(?P<prefix_sub>\d*)?(_S)(?P<suffix_sub>\d+)(\\|/)[A-z]+\-?[A-z]*(?P<prefix_ses>\d*)?(_E)(?P<suffix_ses>\d+)"
prostate_pattern = r"(?:(?:(?:diff?|dwi)(?:\W|_)(?:.*)(?:b\d+))|dif 2000)|(?:adc|Apparent)|prop|blade|fse|tse|^ax T2$"
chunk_pattern = r"_chunk-(?P<chunk>\d)+"
aquisition_date_pattern_comp = re.compile(adquisition_date_pattern_2)
prostate_pattern_comp = re.compile(prostate_pattern, re.I)
chunk_pattern_comp = re.compile(chunk_pattern)
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
    'ImageType': '00080008',
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

options_dcm2niix = "-w 0 -i n -m y -ba n -f %x_%s -z y -g y"

def create_directory_mids_v1(xnat_data_path, mids_data_path, body_part, debug_level):
    print(xnat_data_path)
    print(mids_data_path)
    procedure_class_mr = ProceduresMR()
    procedure_class_light = LightProcedure()
    procedure_class_radiology = RadiologyProcedure()
    for subject_xnat_path in tqdm(xnat_data_path.iterdir()):
        if "_S" not in subject_xnat_path.name:continue
        # num_sessions = len(list(subject_xnat_path.glob('*/')))
        procedure_class_mr.reset_indexes()
        procedure_class_light.reset_indexes()
        for sessions_xnat_path in subject_xnat_path.iterdir():
            if "_E" not in sessions_xnat_path.name: continue
            

            print(sessions_xnat_path)
            findings = re.search(subses_pattern, str(sessions_xnat_path), re.X)
            #print('subject,', findings.group('prefix_sub'), findings.group('suffix_sub'))
            #print('session,', findings.group('prefix_ses'), findings.group('suffix_ses'))
            subject_name = f"sub-{findings.group('prefix_sub')}S{findings.group('suffix_sub')}"
            session_name = f"ses-{findings.group('prefix_ses')}E{findings.group('suffix_ses')}"

            mids_session_path = mids_data_path.joinpath(subject_name, session_name)
            xml_session_rois = list(sessions_xnat_path.rglob('*.xml'))
            #print(f"1: {mids_session_path}")
            tagger = Tagger()
            tagger.load_table_protocol(
                './xnat2mids/protocols/protocol_RM_prostate.tsv'
            )
            if not sessions_xnat_path.joinpath("scans").exists(): continue
            for scans_path in sessions_xnat_path.joinpath("scans").iterdir():
                #if "LOCAL_" not in str(scans_path):
                    
                    # num_jsons = len(list(scans_path.joinpath("resources").rglob("*.dcm"))) #
                    path_dicoms= list(scans_path.joinpath("resources").rglob("*.dcm"))[0].parent
                    print("2"*79)
                    print(path_dicoms)
                    print("2"*79)
                    # folder_conversion = dicom2niix(path_dicoms, options_dcm2niix+ " -b o")
                    
                    # if num_jsons ==0:
                    #     continue
                    # if num_jsons>6:
                    #     path_dicoms= list(scans_path.joinpath("resources").rglob("*.dcm"))[0].parent
                    #     folder_conversion = dicom2niix(path_dicoms, options_dcm2niix) #.joinpath("resources")
                    # else:
                    #     path_dicoms= list(scans_path.joinpath("resources").rglob("*.dcm"))[0].parent
                    #     try:
                    #         folder_conversion = dicom2png(path_dicoms, options_dcm2niix) #.joinpath("resources")
                    #     except RuntimeError as e:
                    #         continue
                    # print("---------", len(list(folder_conversion.iterdir())))
                    # if len(list(folder_conversion.iterdir())) == 0: continue
                    #continue
                    #dict_json = load_json(folder_conversion.joinpath(list(folder_conversion.glob("*.json"))[0]))
                    dict_json = generate_json_dicom(path_dicoms)
                    if debug_level == 2: continue

                    modality = dict_json.get("Modality", "n/a")
                    study_description = dict_json.get("SeriesDescription", "n/a")
                    Protocol_name = dict_json.get("ProtocolName", "n/a")
                    image_type = dict_json.get("ImageType", "n/a")
                    body_part = dict_json.get("BodyPartExamined", body_part)
                    acquisition_date_time = dict_json.get("AcquisitionDateTime", "n/a")
                    acquisition_date = dict_json.get("AcquisitionDate", "n/a")
                    acquisition_time = dict_json.get("AcquisitionTime", "n/a")

                    if (acquisition_date != "n/a") and acquisition_time != "n/a":
                        print("variables separadas")
                        date = str(acquisition_date)
                        time = str(acquisition_time)
                        acquisition_date_time_correct = f"{date[:4]}-{date[4:6]}-{date[6:8]}T{time[:2]}:{time[2:4]}:{time[4:6]}.000"
                        #acquisition_date_time_correct = aquisition_date_pattern_comp.search(acquisition_date_time)
                        #time_values = list(int (x) for x in acquisition_date_time_check.groups())
                    else:
                        if acquisition_date_time == "n/a":
                            acquisition_date_time_correct = "n/a"
                        else:
                            print("variables juntas")
                            acquisition_date_time_check = aquisition_date_pattern_comp.search(acquisition_date_time)
                            try:
                                if acquisition_date_time_check.group("fecha1"):
                                    acquisition_date_time_correct = f'\
{int(acquisition_date_time_check.group("year")):04d}-\
{int(acquisition_date_time_check.group("month")):02d}-\
{int(acquisition_date_time_check.group("day")):02d}T\
{int(acquisition_date_time_check.group("hour")):02d}:\
{int(acquisition_date_time_check.group("minutes")):02d}:\
{int(acquisition_date_time_check.group("seconds")):02d}.\
{int(acquisition_date_time_check.group("ms")):06d}\
'
                                else:
                                    acquisition_date_time_correct = f'\
{int(acquisition_date_time_check.group("year2")):04d}-\
{int(acquisition_date_time_check.group("month2")):02d}-\
{int(acquisition_date_time_check.group("day2")):02d}T\
{int(acquisition_date_time_check.group("hour2")):02d}:\
{int(acquisition_date_time_check.group("minutes2")):02d}:\
{int(acquisition_date_time_check.group("seconds2")):02d}.\
{int(acquisition_date_time_check.group("ms2")):06d}\
'
                            except AttributeError as e:
                                print("error de formato:", acquisition_date_time)
                           

                    # print()
                    # print(modality, study_description, ProtocolName, image_type)
                    print(f"{study_description}")
                    print(f"{Protocol_name}")
                    if modality == "MR":
                        #convert data to nifti
                        folder_conversion = dicom2niix(path_dicoms, options_dcm2niix+ " -b y")
                        # via BIDS protocols
                        searched_prost = prostate_pattern_comp.search(study_description)
                        if searched_prost and "tracew" not in study_description.lower():

                            
                            json_adquisitions = {
                                f'{k}': dict_json.get(k, -1) for k in dict_mr_keys.keys()
                            }
                            try:
                                protocol, acq, task, ce, rec, dir_, part, folder_BIDS = tagger.classification_by_min_max(json_adquisitions)
                                print(protocol, acq, task, ce, rec, dir_, part, folder_BIDS)
                            except EmptyDataError as e:
                                continue
                            procedure_class_mr.control_sequences(
                                folder_conversion, mids_session_path, session_name, protocol, acq, dir_, folder_BIDS, body_part
                            )
                    
                    if modality in ["OP", "SC", "XC", "OT", "SM"]:
                        
                        try:
                            folder_conversion = dicom2png(path_dicoms) #.joinpath("resources")
                        except RuntimeError as e:
                            continue
                        modality_ = ("op" if modality in ["OP", "SC", "XC", "OT"] else "BF")
                        mim = ("mim-ligth" if modality in ["OP", "SC", "XC", "OT"] else "micr")
                        laterality = dict_json.get("Laterality")
                        acq = "" if "ORIGINAL" in image_type else "opacitysubstract"
                        # print(laterality, acq)
                        print("!"*79)
                        print(modality,  mids_session_path.joinpath(mim))
                        print("!"*79)
                        procedure_class_light.control_image(folder_conversion, mids_session_path.joinpath(mim), dict_json, session_name, modality_, acq, laterality, acquisition_date_time_correct)
                    
                    if modality in ["CR", "DX"]:
                        try:
                            folder_conversion = dicom2png(path_dicoms, options_dcm2niix) #.joinpath("resources")
                        except RuntimeError as e:
                            continue
                        modality_ = modality.lower()
                        
                        
                        
                        # print(laterality, acq)
                        print("!"*79)
                        print(modality,  mids_session_path.joinpath(mim))
                        print("!"*79)
                        procedure_class_radiology.control_image(
                            folder_conversion,
                            mids_session_path,
                            dict_json,
                            session_name,
                            modality_,
                            acquisition_date_time_correct
                        )
                    
        if debug_level == 3: continue
        procedure_class_mr.copy_sessions(subject_name)
        procedure_class_light.copy_sessions(subject_name)


participants_header = ['participant_id', 'participant_pseudo_id', 'modalities', 'body_parts', 'patient_birthday', 'age', 'gender']
participants_keys = ['PatientID','Modality', 'BodyPartExamined', 'PatientBirthDate', 'PatientSex', 'AcquisitionDateTime']
session_header = ['session_id','session_pseudo_id', 'acquisition_date_Time','radiology_report']
sessions_keys = ['AccessionNumber', 'AcquisitionDateTime']
scans_header = [
    'scan_file','BodyPart',
    'Manufacturer','ManufacturersModelName','DeviceSerialNumber',
    'MagneticFieldStrength','ReceiveCoilName','PulseSequenceType',
    'ScanningSequence','SequenceVariant','ScanOptions','SequenceName','PulseSequenceDetails','MRAcquisitionType',
    'EchoTime','InversionTime','SliceTiming','SliceEncodingDirection','FlipAngle'
]

def create_tsvs(xnat_data_path, mids_data_path, body_part_aux):
    """
        This function allows the user to create a table in format ".tsv"
        whit a information of subject
        """
    
    list_information= []
    for subject_path in mids_data_path.glob('*/'):
        if not subject_path.match("sub-*"): continue
        subject = subject_path.parts[-1]
        
        old_subject = "_".join([
                subject.split("-")[-1].split("S")[0],
                "S"+subject.split("-")[-1].split("S")[1]
            ])
        list_sessions_information = []
        for session_path in subject_path.glob('*/'):
            if not session_path.match("ses-*"): continue
            session = session_path.parts[-1]
            
            old_sesion = "_".join([
                session.split("-")[-1].split("E")[0],
                "E"+session.split("-")[-1].split("E")[1]
            ])
            
            modalities = []
            body_parts = []
            patient_birthday = None
            patient_ages = list([])
            patient_sex = None
            adquisition_date_time = None
            report_path = list(xnat_data_path.glob(f'*{old_subject}/*{old_sesion}/**/*.txt'))
            
            if not report_path:
                report="n/a"
            else:
                with report_path[0].open("r", encoding="iso-8859-1") as file_:
                    report = file_.read()
                report = report.replace("\t", "    ")
            list_scan_information = []
            for json_pathfile in subject_path.glob('**/*.json'):
                chunk_search = chunk_pattern_comp.search(json_pathfile.stem)
                print()
                if chunk_search:
                    list_nifties = json_pathfile.parent.glob(
                        chunk_pattern_comp.sub(
                            "*", 
                            json_pathfile.stem
                        ) + "*"
                    )
                else:
                    list_nifties = json_pathfile.parent.glob(
                        json_pathfile.stem + "*"
                    )
                list_nifties = [f for f in list_nifties if "json" not in f.suffix]
                print(list(list_nifties))
                json_file = load_json(json_pathfile)
                pseudo_id = json_file[participants_keys[0]]
                modalities.append(json_file[participants_keys[1]])
                
                try:
                    body_parts.append(json_file[participants_keys[2]].lower())
                except KeyError as e:
                    body_parts.append(body_part_aux.lower())
                try:
                    print(json_file[participants_keys[3]])
                    patient_birthday = datetime.fromisoformat(json_file[participants_keys[3]])
                except KeyError as e:
                    patient_birthday = "n/a"
                except ValueError as e:
                    birtday = json_file[participants_keys[3]]
                    if birtday:
                        correct_birtday = f"{birtday[0:4]}-{birtday[4:6]}-{birtday[6:8]}"
                        patient_birthday = datetime.fromisoformat(correct_birtday)
                    else:
                        patient_birthday = "n/a"
                try:
                    patient_sex = json_file[participants_keys[4]]
                except KeyError as e:
                    patient_sex = "n/a"
                acquisition_date_time = json_file.get("AcquisitionDateTime", "n/a")
                acquisition_date = json_file.get("AcquisitionDate", "n/a")
                acquisition_time = json_file.get("AcquisitionTime", "n/a")

                if (acquisition_date != "n/a") and acquisition_time != "n/a":
                    print("variables separadas")
                    date = str(acquisition_date)
                    time = str(acquisition_time)
                    acquisition_date_time_correct = f"{date[:4]}-{date[4:6]}-{date[6:8]}T{time[:2]}:{time[2:4]}:{time[4:6]}.000"
                    #acquisition_date_time_correct = aquisition_date_pattern_comp.search(acquisition_date_time)
                    #time_values = list(int (x) for x in acquisition_date_time_check.groups())
                else:
                    if acquisition_date_time == "n/a":
                        acquisition_date_time_correct = "n/a"
                    else:
                        print("variables juntas")
                        acquisition_date_time_check = aquisition_date_pattern_comp.search(acquisition_date_time)
                        try:
                            if acquisition_date_time_check.group("fecha1"):
                                    acquisition_date_time_correct = f'\
{int(acquisition_date_time_check.group("year")):04d}-\
{int(acquisition_date_time_check.group("month")):02d}-\
{int(acquisition_date_time_check.group("day")):02d}T\
{int(acquisition_date_time_check.group("hour")):02d}:\
{int(acquisition_date_time_check.group("minutes")):02d}:\
{int(acquisition_date_time_check.group("seconds")):02d}.\
{int(acquisition_date_time_check.group("ms")):06d}\
'
                            else:
                                acquisition_date_time_correct = f'\
{int(acquisition_date_time_check.group("year2")):04d}-\
{int(acquisition_date_time_check.group("month2")):02d}-\
{int(acquisition_date_time_check.group("day2")):02d}T\
{int(acquisition_date_time_check.group("hour2")):02d}:\
{int(acquisition_date_time_check.group("minutes2")):02d}:\
{int(acquisition_date_time_check.group("seconds2")):02d}.\
{int(acquisition_date_time_check.group("ms2")):06d}\
'
                        except AttributeError as e:
                            print("error de formato:", acquisition_date_time)
                        

#                 if (acquisition_date != "n/a") and acquisition_time != "n/a":
#                     acquisition_date_time = str(acquisition_date) + str(acquisition_time)
#                     acquisition_date_time_check = aquisition_date_pattern_comp.search(acquisition_date_time)
#                 else:
#                     if acquisition_date_time == "n/a":
#                         acquisition_date_time_correct = "n/a"
#                     else:
#                         acquisition_date_time_check = aquisition_date_pattern_comp.search(acquisition_date_time)
#                 try:
#                     time_values = list(int (x) for x in acquisition_date_time_check.groups())
#                 except AttributeError as e:
#                     print("error de formato")
                
#                 # acquisition_date_time_check = aquisition_date_pattern_comp.search(json_file[participants_keys[5]])

#                 try:
#                     time_values = list(int (x) for x in acquisition_date_time_check.groups())
#                 except AttributeError as e:
#                         continue
#                 acquisition_date_time_correct = f"\
# {time_values[0]:04d}-\
# {time_values[1]:02d}-\
# {time_values[2]:02d}T\
# {time_values[3]:02d}:\
# {time_values[4]:02d}:\
# {time_values[5]:02d}.\
# {time_values[6]:06d}\
# "
                adquisition_date_time = datetime.fromisoformat(acquisition_date_time_correct)
                # adquisition_date_time = datetime.fromisoformat(json_file[participants_keys[5]].split('T')[0])
                if patient_birthday != "n/a":
                    patient_ages.append(int((adquisition_date_time - patient_birthday).days / (365.25)))

                if json_file[participants_keys[1]] == 'MR':
                    list_scan_information.append({
                        key:value
                        for nifti in list_nifties for key, value in zip(
                            scans_header,
                            [
                                str(Path(".").joinpath(*nifti.parts[-2:])),
                                 body_parts[-1], 
                                 *[json_file.get(key, "n/a") for key in scans_header[2:]]
                            ]
                        ) 
                        

                    })
                #print(list_scan_information)
                # if json_file[participants_keys[1]] in ["OP", "SC", "XC", "OT", "SM"]:
                #     procedure_class_light.create_row_tsv()

            patient_ages = sorted(list(set(patient_ages)))
            modalities = sorted(list(set(modalities)))
            body_parts = sorted(list(set(body_parts)))
            try:
                accesion_number =  json_file['AccessionNumber']
            except KeyError:
                accesion_number = "n/a"
            list_sessions_information.append({
                 key:value
                 for key, value in zip(
                    session_header,
                    [session, accesion_number, str(adquisition_date_time), report]
                 )

            })
            pandas.DataFrame.from_dict(list_scan_information).to_csv(
                session_path.joinpath(f"{subject}_{session}_scans.tsv"), sep="\t", index=False
            )
        pandas.DataFrame.from_dict(list_sessions_information).to_csv(
            subject_path.joinpath(f"{subject}_sessions.tsv"), sep="\t", index=False
        )
        list_information.append({
            key:value
            for key, value in zip(
                participants_header,
                [subject, pseudo_id, modalities, body_parts, (str(patient_birthday.date()) if patient_birthday != "n/a" else patient_birthday), patient_ages, patient_sex]
            )
        })
    print(list_information)
    pandas.DataFrame.from_dict(list_information).to_csv(
        mids_data_path.joinpath("participants.tsv"), sep="\t", index=False
    )