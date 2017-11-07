--register piggybank lib

REGISTER /home/acadgild/piggybank-0.15.0.jar
REGISTER /home/acadgild/stateWise.jar;

--defining xml-xpath for importing datas

DEFINE XPath org.apache.pig.piggybank.evaluation.xml.XPath();

data =  LOAD '/pig/state.xml' using org.apache.pig.piggybank.storage.XMLLoader('row') as (x:chararray);

--reading proper values for computation from xml file

State = FOREACH data GENERATE XPath(x,'row/State_Name') AS statename, XPath(x,'row/District_Name') AS disname,XPath(x,'row/Project_Objectives_IHHL_BPL') AS BPL,XPath(x,'row/Project_Objectives_IHHL_TOTAL') AS total ;


--creating temporary function

DEFINE func pigudf.Calc;

--filtering as per logic

Obj = FILTER State BY func(BPL,total);

--store result

STORE Obj INTO '/vishnu/pigudf' USING PigStorage(','); 


