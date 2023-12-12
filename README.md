
<div class="clearfix" style="padding: 0px; padding-left: 100px; display: flex; flex-wrap: nowrap; justify-content: space-evenly; align-items:center">
<a href="http://bimcv.cipf.es/"><img src="https://github.com/jomasator2/MIDS/blob/main/images/Logo_UMIB.png" width="300px" style="display: inline-block; "></a><a href="http://fisabio.san.gva.es/"><img src="https://github.com/jomasator2/MIDS/blob/main/images/logo_fisabio.png" width="330px" class="pull-right" style="display: inline-block;"></a><a href="https://san.gva.es/"><img src="https://github.com/jomasator2/MIDS/blob/main/images/logo_gva.png" width="200px" class="center-block" style=" display: inline-block;"></a>
</div>
<br></br>

<center> <h1>Medical Imaging Bank Valencia Region 1</h1> </center>
<!--<h1 style="text-align:center">Medical Imaging Bank Valencia Region</h1>-->
<h2 style="text-align:center">Medical Imaging Data Structure</h2>

<p style="text-align:justify">
In order to add to the scientific knowledge, methods that yield reliable and reproducible results must be used.
High test-retest reliability of the methods applied is the foundation of research irrespective of the scientific discipline and it is in the prime interest of every scientist that results are reproducible. While such reproducibility was considered of utmost importance in the positron emitting tomography (PET) field [<span style="color:gray">Adams et al., 2010</span>], the quantitative assessment of reproducibility has largely been neglected in the fMRI community,
or as Bennett and Miller described it: “Reliability is not a typical topic of conversation” between fMRI investigators [<span style="color:gray">Bennett and Miller, 2010</span>]. This situation changed significantly in 2016, following the establishment of the Committee on Best Practices in Data Analysis and Sharing ([COBIDAS](www.humanbrainmapping.org/cobidas/)) by the most important neuroimaging society - the Organisation for Human Brain Mapping (OHBM).
</p>

<p style="text-align:justify">
The objective of BIMCV (Medical Imaging Bank Valencia Region) is that all this scientific knowledge be colected in a correct and efficient way. More concretely, all that knowledge is refered to data imaging. Find a way to organize this information is necesary.
</p>

<p style="text-align:justify">
Currently, distinc forms to store images and medical information exist, but there is not a standard that indicates how this information should be organized and shared. The CEIB idea is to use a simple organisation that any researcher can understand the distribution of data.
</p>

<p style="text-align:justify">
This desing is called MIDS (Medical Imaging Data Structure). MIDS is a new standard that contains every type of medical information and images in simple hierarchy folders. This is born as an extension of the standard BIDS (Brain Imaging Data Structure). BIDS [<span style="color:gray">Gorgolewski et al.</span>] is a structure that collect medical brain images, but MIDS pretend to extend it far away and not limit it the use to medical brain images. the idea is to create the same structure with both brain, column images or torso,... whether Resonance magnetic, Computed tomography, ecography,... This structure will definitely follow the same process regardless of the type and shape of the image.
</p>


## Index

>[1. Introduction: Medical Imaging Data Structure](#loading)

>[2. MIDS structural format](#mids)

>[3. Aplications](#aplications)

>[4. future lines](#future)

>[5. References](#references)

<!--[A. Annex 1: licences](#annex1)-->

<a id='loading'></a>
## 1. Introduction

<p style="text-align:justify">
At present, many studies based on obtaining a dataset of medical images for its process of study. the qualitative improvement has been enormous for these studies since its use. Management and control all images and metadata added to them is a extra hard work. Furthermore, during the study, more data is generated where it is necessary to relocate. Each study search the way to organize these data in the way that best suits. this makes it more difficult to understand the data collected and results.
</p>

<p style="text-align:justify">
There are any studies that proposal a standar to store this type of data. One of them is BIDS ([Brain Imaging Data Structure](http://bids.neuroimaging.io/),[1]). BIDS is a proposal standar to store a magnetic resonance imaging and data in a structural folder hierarchy. This structure is very clear and easy to use. This standard is supported by several programs and libraries dedicated to the study of medical images (i.e. c-pacs, freesurfer, xnat, BIDSValidator...) and it is very used in the comunity researchers. In te Figure 1, a example of structure in BIDS is presented,the left directory is a folder with dicom images (<span style="color:gray">Mildenberger, Eichelberg & Martin,2002</span>) and the right directory is a BIDS structure
</p>



<p style="text-align:center">
<img src="./images/dicom2bids.png" >
</p>
<div style='text-align:center;'>
figure 1: DICOM to BIDS conversion with the tool Dcm2Bids
</div>


<p style="text-align:justify">
However, BIDS only suports RM imaging and only brain images. For example, if a project need lumbar RM imaging, BIDS would not given support to these images. Nevertheless, this structure has not limits to store other anatomy parts. Without going into further, other acquisition images can be implemented. To that end, MIDS has been created.
</p>

<a id='mids'></a>
## 2. MIDS structural format


<p style="text-align:justify">
Currently, MIDS tries to stablish yourself like a BIDS ampliation. the structural format is highly similar like BIDS in magnetic resonance.The paper of [standar BIDS](http://www.nature.com/articles/sdata201644) or [BIDS specification](http://bids.neuroimaging.io/bids_spec1.0.2.pdf) can be read for more detail.
</p>

<p style="text-align:justify">
One proposal to extend and include BIDS into MIDS is create new variable in named specification of files. For example the estructure in one file is **sub-*id*[*obtative\_list\_variables*]\_*modality*.nii.gz** the proposal is add in this obtative variables the term \_bop-*body\_part\_dicom* which refers to the part of the body collected in the label dicom [(0018,0015)](http://dicomlookup.com/lookup.asp?sw=Tnumber&q=(0018,0015). The absence of the bop variable is set as the default skull.Thus, any BIDS structure is included in MIDS. The folder **data** shows a one exemple of input directory and output directory in MIDS structure.</p>

<a id='aplications'></a>
# Aplications

## XNAT2MIDS
This sorfware allow the user to Download one project into XNAT platform (--version XNAT <= 1.7.4.1) of BIMCV and convert the XNAT directory images in a directory MIDS. The aplication execution need Python --version >= 3.5.

An example of execution is:

```sh
python3 -m xnat2mids -i DIR_TO_SAVE -w XNAT_WEB -u USER -b BODYPART -v
```

### Options



arguments:

  +  **-w (--web) XNAT_WEB** The ULR page where XNAT is.
  + **-i (--input) PATH** The directory where the the project will created.
  + **-u (--user) [USER]** The username to loggin in XNAT. If not write a username, It loggin as guest.
  + **-bp (--body-part) [BODY_PART]** Optional argument to clarify which part of the body are in the images. This argument is only used if the body part dicom tag is not found in the image.
  + **-v (--verbose)** 

  If the xnat project is already downloaded. you can run the command by removing the -w and -u arguments.

<a id='During Execution'></a>
## 4. During Execution
When we execute the programme, it will ask for the xnat user (if it was not set as an argument) and the password of our user.


Then The program will ask us which projects we want to pass to BIDS/MIDS. Just enter the numbers associated with the projects separated with space.

    user: AAAAAAA
    password:

    1) p0012023         2) p0022021         3) p0022022         4) p0022023         5) p0032021         
    6) p0032023         7) p0042021         8) p0052021         9) p0052022         10) p0062021         
     
    Choose the project: 1 3 4 

<a id='future'></a>
## 4. Future Lines

Now, BIDS is a potential standard to store images of RM and there is a little diference between BIDS and MIDS. However,MIDS pretend to incorporate, within epidemiological studies based on Population Image (BIMCV), any type of [modality](http://dicomlookup.com/modalities.asp) of medical image (i.e. Computed Radiography,Computed Tomography, Ultrasound, Mammography, ...).

Taking in count, all images  in BIMCV come from clinical imaging protocols in RM. these imaging don't ajust to labels in BIDS and, for that reason, one proposal is to extend and specify format label in MIDS. this format structure is "\_OriModSeq" where "Ori" is the orientation (**ax**->axial,**sag**->sagital,**cor**->coronal), "Mod" is the modality of image (T1,T2,Stir,angio,...) and "Seq" is the scaning sequence (**SE**->Spin Echo,**FSE**->Fast Spin Echo,**GR**->Gradiente,...). For more information, see the table in ./MIDS/XNAT2MIDS/.python\_objects/dictionary\_scan.csv in the column _Modality\_label_.

<a id='references'></a>
## 5. References

Adams MC., Turkington TG., Wilson JM., Wong TZ. (2010). A systematic review of the factors affecting accuracy of SUV measurements. AJR American journal of roentgenology 195:310-320.

Bennett CM., Miller MB. (2010) How reliable are the results from functional magnetic resonance imaging? Annals of the New York Academy of Sciences 1191:133-155.

Mildenberger, P., Eichelberg, M., & Martin, E. (2002). Introduction to the DICOM standard. European radiology, 12(4), 920-927.

Gorgolewski, K. J., Auer, T., Calhoun, V. D., Craddock, R. C., Das, S., Duff, E. P., ... & Handwerker, D. A. (2016). The brain imaging data structure, a format for organizing and describing outputs of neuroimaging experiments. Scientific Data, 3, 160044.
