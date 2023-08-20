********************
Authors:
- Mary Penne Mays
- Siddharth Satyakam
- Sravani Chandakaq
- Lav Patel
********************;

DM'output;clear;log;clear;';

OPTIONS PAGENO=1 NODATE dsoptions=nomissopt;

/* Specify the path for .sas7bdat file */
%let filepath = "./import/temp/merged/pkd_registry.sas7bdat";

/* PROC IMPORT to load the .sas7bdat file */
proc import datafile=&filepath
     out=pkd_registry /* Specify the name for pkd_registry SAS dataset */
     dbms=SAS
     REPLACE;
     GETNAMES=yes;
run;

**********************************************
PKD MULTI REGISTRY'S
1. The CONTENTS Procedure
2. Alphabetic List of Variables and Attributes
3. Engine/Host Dependent Information
**********************************************;
proc contents DATA = pkd_registry; 
run;

**************************
PKD MULTI REGISTRY DATASET
**************************;
data pkd_dataset;
    SET pkd_registry;
run;

*****************************************
PRINT PKD MULTI REGISTRY TABLE OF CONTENT
*****************************************;
proc print DATA = pkd_dataset; 
run;
