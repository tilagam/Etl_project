----Emp_ETL Testing scripts

USE EMP_ETL_V1;


select * from EMP_DTS_SRC1;--Source table

select * from EMP_DTS_SRC2;--Source table

select * from EMP_DTS_SRC3;--Source table

select * from EMP_DTS_TRG ;--Target table

----Test Data
INSERT INTO EMP_DTS_SRC1  VALUES ('1','SMITH','JOHN','A',2000,'1991-11-01','Male','2017-04-09');
INSERT INTO EMP_DTS_SRC1 VALUES ('2','HERB','PETER','S',5000,'1996-11-22','Female','2013-04-09');
INSERT INTO EMP_DTS_SRC1 VALUES ('3','WATSON','TOM','D',600,'1993-12-02','MALE','2010-04-09');
INSERT INTO EMP_DTS_SRC1 VALUES ('4','McCULLUM','BILL','U',8000,'1990-12-02','female','2015-04-09');
INSERT INTO EMP_DTS_SRC1 VALUES ('5','CARTER','SAM','N',9000,'1993-12-02','female','2018-04-09');
INSERT INTO EMP_DTS_SRC1 VALUES ('6','DAIL','SAM','N',9000,'1993-12-02','female','2018-04-09');

INSERT INTO EMP_DTS_SRC2 VALUES ('1','BRYYANT','WILL','Z',9000,'1993-12-02','F','2018-04-09');
INSERT INTO EMP_DTS_SRC2 VALUES ('2','CURRAN','KATE','B',500,'1978-12-02','f','2001-04-09');
INSERT INTO EMP_DTS_SRC2 VALUES ('3','ROSS','EMMA','H',8000,'1993-12-02','M','2018-04-09');
    
INSERT INTO EMP_DTS_SRC3 VALUES ('1','ROSS','ALEX','F',7000,'1978-12-02','1','1999-04-09');    
INSERT INTO EMP_DTS_SRC3 VALUES ('2','ROSS','ALEX','F',700,'1978-12-02','1','1999-04-09');
INSERT INTO EMP_DTS_SRC3 VALUES ('3','TAYLOR','JIMMY','P',4000,'1989-12-02','2','2011-04-09');
INSERT INTO EMP_DTS_SRC3 VALUES ('4','LISTER','BEN','O',4000,'1993-12-02','1','2018-04-09');
INSERT INTO EMP_DTS_SRC3 VALUES ('5','X','Y','Z',9000,'1993-12-02','1','2018-04-09');





----------------------------
Truncate table EMP_DTS_SRC1 ;
Truncate table EMP_DTS_SRC2 ;
Truncate table EMP_DTS_SRC3 ;
Truncate table EMP_DTS_TRG ;


  




--1) To validate ETL Job for 'EMP_DTS_SRC1' table.

--To check data in Source tables
select * from EMP_DTS_SRC1;--Source table

select * from EMP_DTS_SRC2;--Source table

select * from EMP_DTS_SRC3;--Source table

Select * from EMP_DTS_TRG ; --target table




--2) To validate record count between EMP_DTS_SRC1 and EMP_DTS_TRG.

select COUNT(*)as COUNT_OF_EMP_DTS_SRC1 from EMP_DTS_SRC1 
Where EMP_SALARY >= 1000 ; 

select COUNT(*) as COUNT_OF_EMP_DTS_TRG from EMP_DTS_TRG
Where REFERENCE_TABLE = 'EMP_DTS_SRC1' ; 




--3) To validate Null count between 'EMP_DTS_SRC1 and  EMP_DTS_TRG.

Select 
Count (*)-Count(EMP_ID)EMP_ID	,
Count (*)-Count(EMP_CODE)EMP_CODE	,
Count (*)-Count(LAST_NAME)LAST_NAME	,
Count (*)-Count(FIRST_NAME)FIRST_NAME	,
Count (*)-Count(MIDDLE_NAME)MIDDLE_NAME	,
Count (*)-Count(EMP_SALARY)EMP_SALARY	,
Count (*)-Count(EMP_DOB)EMP_DOB	,
Count (*)-Count(EMP_GENDER)EMP_GENDER	,
Count (*)-Count(HIRE_DATE)HIRE_DATE	
From EMP_DTS_SRC1
Where EMP_SALARY >= 1000 ; 
	
 
Select 
Count (*)-Count(ID)ID	,
Count (*)-Count(EMP_CODE)EMP_CODE	,
Count (*)-Count(EMP_NAME)EMP_NAME	,
Count (*)-Count(EMP_SALARY)EMP_SALARY	,
Count (*)-Count(EMP_DOB)EMP_DOB	,
Count (*)-Count(EMP_GENDER)EMP_GENDER	,
Count (*)-Count(HIRE_DATE)HIRE_DATE	,
Count (*)-Count(TOTAL_EXP)TOTAL_EXP	,
Count (*)-Count(REFERENCE_TABLE)REFERENCE_TABLE	,
Count (*)-Count(TIMESTAMP)TIMESTAMP	
From EMP_DTS_TRG 
Where REFERENCE_TABLE = 'EMP_DTS_SRC1' ; 



--4) To validate duplicate record in EMP_DTS_TRG Table.

Select EMP_CODE, COUNT(*) from EMP_DTS_TRG
Group by EMP_CODE
Having count (*) > 1 ; 


--Delete from EMP_DTS_TRG
--WHERE id NOT IN (SELECT MAX(id) FROM EMP_DTS_TRG GROUP BY EMP_CODE) ;


--5) To validate rejected record in EMP_DTS_TRG Table.

SeleCt * from EMP_DTS_SRC1
Where EMP_SALARY < 1000;

SeleCt * from EMP_DTS_TRG
Where EMP_SALARY < 1000;


-- 6) To validate Auto generated field "Id" in EMP_DTS_TRG Table.


Select * from EMP_DTS_TRG;

Select COUNT(*) Total_COUNT from EMP_DTS_TRG;

Select MAX(ID) MAX_ID from EMP_DTS_TRG;


--7) To validation  direct Mapping of EMP_CODE, EMP_DOB, HIRE_Date Column.


Select EMP_CODE, EMP_DOB, HIRE_DATE from EMP_DTS_SRC1
Where EMP_SALARY > = 1000 

--Except

Select EMP_CODE, EMP_DOB, HIRE_DATE from EMP_DTS_TRG
Where REFERENCE_TABLE = 'EMP_DTS_SRC1';





--8) To validation concatenation of Last Name, First Name, Middle Name in EMP_DTS_Trg Table. 
--(Middle name present)


--Insert data in source

Select * from EMP_DTS_SRC1

INSERT INTO [dbo].[EMP_DTS_SRC1]
           ([EMP_ID]
           ,[LAST_NAME]
           ,[FIRST_NAME]
           ,[MIDDLE_NAME]
           ,[EMP_SALARY]
           ,[EMP_DOB]
           ,[EMP_GENDER]
           ,[HIRE_DATE])
     VALUES
           (7
           ,'Patil'
           ,'Amit'
           ,'G'
           , '1000'
           , '1991-11-01 00:00:00.000'
           ,'Male'
           ,'2017-04-09')
GO


--Check the data in Source table and target table
Select * from EMP_DTS_SRC1 

Select * from EMP_DTS_TRG ;




--Covert Source table's data which is expected in target table and compare both.
Select EMP_CODE, (LAST_NAME+ ' ' +FIRST_NAME+ ' ' + ISNULL (MIDDLE_NAME, ''))EMP_NAME from EMP_DTS_SRC1
Where EMP_SALARY > = 1000 --and EMP_CODE = 'EMP07'

Except

Select EMP_CODE, EMP_NAME from EMP_DTS_TRG 
Where REFERENCE_TABLE = 'EMP_DTS_SRC1' --and EMP_CODE = 'EMP07' ; 





--9) To validation concatenation of Last Name, First Name, Middle Name in EMP_DTS_Trg Table.
--(Middle name Absent)
 

 --Insert data in source

Select * from EMP_DTS_SRC1

INSERT INTO [dbo].[EMP_DTS_SRC1]
           ([EMP_ID]
           ,[LAST_NAME]
           ,[FIRST_NAME]
           --,[MIDDLE_NAME]
           ,[EMP_SALARY]
           ,[EMP_DOB]
           ,[EMP_GENDER]
           ,[HIRE_DATE])
     VALUES
           (10
           ,'D'
           ,'John'
          -- ,'G'
           , '1000'
           , '1993-11-01 00:00:00.000'
           ,'Male'
           ,'2018-04-09')
GO


--Check the data in Source table and target table
Select * from EMP_DTS_SRC1 

Select * from EMP_DTS_TRG ;




--Covert Source table's data which is expected in target table and compare both.
Select EMP_CODE,(LAST_NAME+ ' ' +FIRST_NAME+ ' ' + ISNULL (MIDDLE_NAME, ''))EMP_NAME from EMP_DTS_SRC1
Where EMP_CODE = 'EMP010'
--EMP_SALARY > = 1000

--Except

Select EMP_CODE, EMP_NAME from EMP_DTS_TRG 
Where EMP_CODE = 'EMP010'
--REFERENCE_TABLE = 'EMP_DTS_SRC1'; 






--10) To validation EMP_Salary Transformation when salary is less than 1000
Select * from EMP_DTS_SRC1 
Where EMP_SALARY < 1000 ; 

Select * from EMP_DTS_TRG 
Where EMP_SALARY < 1000 ; 

--11)To validation EMP_Salary Transformation when salary is equals to  1000
Select * from EMP_DTS_SRC1 
Where EMP_SALARY = 1000 ; 

Select * from EMP_DTS_TRG 
Where EMP_SALARY = 1000 ; 


-- 12)To validation EMP_Salary Transformation when salary is greater than  1000


Select * from EMP_DTS_SRC1 
Where EMP_SALARY > 1000 ; 

Select * from EMP_DTS_TRG 
Where EMP_SALARY > 1000 ; 





--13),14) To validation EMP_GENDER Transformation for 'Male'.

--Male -- >M
--Female --> F

Select EMP_CODE, EMP_SALARY, ---EMP_GENDER, ,

(Case when EMP_GENDER = 'Male' Then 'M'
	 When EMP_GENDER = 'Female' Then 'F'

END) EMP_GENDER_Cov
from EMP_DTS_SRC1 
Where EMP_SALARY >=  1000 


Except


Select EMP_CODE, EMP_SALARY , EMP_GENDER from EMP_DTS_TRG 
Where REFERENCE_TABLE = 'EMP_DTS_SRC1' ; 




--15) Unknown




--16 ) To validate  Total_Exp when current year is equal to hire year.

----Delete all data in srouce and target table
Truncate table EMP_DTS_SRC1 ;
Truncate table EMP_DTS_SRC2 ;
Truncate table EMP_DTS_SRC3 ;
Truncate table EMP_DTS_TRG ;


--Insert test data in source table

INSERT INTO [dbo].[EMP_DTS_SRC1]
           ([EMP_ID]
           ,[LAST_NAME]
           ,[FIRST_NAME]
           --,[MIDDLE_NAME]
           ,[EMP_SALARY]
           ,[EMP_DOB]
           ,[EMP_GENDER]
           ,[HIRE_DATE])
     VALUES
           (10
           ,'D'
           ,'John'
          -- ,'G'
           , '1000'
           , '1993-11-01 00:00:00.000'
           ,'Male'
           ,'2021-03-31')
GO




--Check data in source and target table

Select * from EMP_DTS_SRC1 ; 


Select * from EMP_DTS_TRG ;  





 
 ---17 ) To validate   Total_Exp when current year is greater to hire year.

 
----Delete all data in srouce and target table
Truncate table EMP_DTS_SRC1 ;
Truncate table EMP_DTS_SRC2 ;
Truncate table EMP_DTS_SRC3 ;
Truncate table EMP_DTS_TRG ;


--Insert test data in source table

INSERT INTO [dbo].[EMP_DTS_SRC1]
           ([EMP_ID]
           ,[LAST_NAME]
           ,[FIRST_NAME]
           ,[MIDDLE_NAME]
           ,[EMP_SALARY]
           ,[EMP_DOB]
           ,[EMP_GENDER]
           ,[HIRE_DATE])
     VALUES
           (11
           ,'D'
           ,'Ajay'
          ,'G'
           , '1000'
           , '1993-11-01 00:00:00.000'
           ,'Male'
           ,'2020-03-31')
GO




--Check data in source and target table

Select * from EMP_DTS_SRC1 ; 


Select * from EMP_DTS_TRG ;  



--18)To validate  Total_Exp when current year is less to hire year

----Delete all data in srouce and target table
Truncate table EMP_DTS_SRC1 ;
Truncate table EMP_DTS_SRC2 ;
Truncate table EMP_DTS_SRC3 ;
Truncate table EMP_DTS_TRG ;


--Insert test data in source table

INSERT INTO [dbo].[EMP_DTS_SRC1]
           ([EMP_ID]
           ,[LAST_NAME]
           ,[FIRST_NAME]
           ,[MIDDLE_NAME]
           ,[EMP_SALARY]
           ,[EMP_DOB]
           ,[EMP_GENDER]
           ,[HIRE_DATE])
     VALUES
           (11
           ,'D'
           ,'Ajay'
          ,'G'
           , '1000'
           , '1993-11-01 00:00:00.000'
           ,'Male'
           ,'2022-03-31')
GO




--Check data in source and target table

Select * from EMP_DTS_SRC1 ; 


Select * from EMP_DTS_TRG ;  



--19) To validate Total_Exp when diffrence between current year and  hire year is more than 99.

----Delete all data in srouce and target table
Truncate table EMP_DTS_SRC1 ;
Truncate table EMP_DTS_SRC2 ;
Truncate table EMP_DTS_SRC3 ;
Truncate table EMP_DTS_TRG ;


--Insert test data in source table

INSERT INTO [dbo].[EMP_DTS_SRC1]
           ([EMP_ID]
           ,[LAST_NAME]
           ,[FIRST_NAME]
           ,[MIDDLE_NAME]
           ,[EMP_SALARY]
           ,[EMP_DOB]
           ,[EMP_GENDER]
           ,[HIRE_DATE])
     VALUES
           (12
           ,'D'
           ,'Rohit'
          ,'S'
           , '1000'
           , '1993-11-01 00:00:00.000'
           ,'Male'
           ,'1920-03-31')
GO




--Check data in source and target table

Select * from EMP_DTS_SRC1 ; 

Select * from EMP_DTS_TRG ; 




--20) To validate reference table column in EMP_DTS_TRG
--21) To validate reference table column in EMP_DTS_TRG

----Delete all data in srouce and target table
Truncate table EMP_DTS_SRC1 ;
Truncate table EMP_DTS_SRC2 ;
Truncate table EMP_DTS_SRC3 ;
Truncate table EMP_DTS_TRG ;



---Insert data in source tables (ETL code automatically executred and data goes to target table)

INSERT INTO EMP_DTS_SRC1  VALUES ('1','SMITH','JOHN','A',2000,'1991-11-01','Male','2017-04-09');
INSERT INTO EMP_DTS_SRC2 VALUES ('1','BRYYANT','WILL','Z',9000,'1993-12-02','F','2018-04-09');
INSERT INTO EMP_DTS_SRC3 VALUES ('1','ROSS','ALEX','F',7000,'1978-12-02','1','1999-04-09'); 




Select * from EMP_DTS_SRC1;
Select * from EMP_DTS_SRC2;
Select * from EMP_DTS_SRC3;

Select * from EMP_DTS_TRG ; 




 
 ---To check SCDs of EMP_DTS_SRC1

--22),23)24)25) 
--To check SCD transformation in EMP_DTS_TRG  update   First_Name in EMP_DTS_SRC1.

--Delete all data in source and target table
Truncate table EMP_DTS_SRC1 ;
Truncate table EMP_DTS_SRC2 ;
Truncate table EMP_DTS_SRC3 ;
Truncate table EMP_DTS_TRG ;



--Insert test data in source table

INSERT INTO [dbo].[EMP_DTS_SRC1] ([EMP_ID],[LAST_NAME],[FIRST_NAME],[MIDDLE_NAME],[EMP_SALARY],[EMP_DOB],[EMP_GENDER],[HIRE_DATE])
     VALUES
           (1,'D','Rohit','S', '1000', '1993-11-01 00:00:00.000','Male','1920-03-31')
GO



--Check data Source and target table

Select * from EMP_DTS_SRC1;

Select * from EMP_DTS_TRG ; 


----Update First name  in source table

Update EMP_DTS_SRC1 set FIRST_NAME  = 'Rahul'
where EMP_CODE = 'EMP01' ;


----Update MIddle name in source table

Update EMP_DTS_SRC1 set LAST_NAME  = 'B'
where EMP_CODE = 'EMP01' ;




----Update MIddle name in source table

Update EMP_DTS_SRC1 set MIDDLE_NAME  = 'J'
where EMP_CODE = 'EMP01' ;


----Update middle name 'null' in source table

Update EMP_DTS_SRC1 set MIDDLE_NAME  = NULL
where EMP_CODE = 'EMP01' ;




--Check data Source and target table

Select * from EMP_DTS_SRC1;

Select * from EMP_DTS_TRG ; 



--26) To check SCD Transformation in EMP_DTS_TRG  update records having EMP_SALARY is less than 1000


--Delete all data in source and target table
Truncate table EMP_DTS_SRC1 ;
Truncate table EMP_DTS_SRC2 ;
Truncate table EMP_DTS_SRC3 ;
Truncate table EMP_DTS_TRG ;



--Insert test data in source table

INSERT INTO [dbo].[EMP_DTS_SRC1] ([EMP_ID],[LAST_NAME],[FIRST_NAME],[MIDDLE_NAME],[EMP_SALARY],[EMP_DOB],[EMP_GENDER],[HIRE_DATE])
     VALUES
           (1,'D','Rohit','S', '1000', '1993-11-01 00:00:00.000','Male','2017-03-31')
GO



--Check data Source and target table

Select * from EMP_DTS_SRC1;

Select * from EMP_DTS_TRG ; 


----Update salary in source table

Update EMP_DTS_SRC1 set EMP_SALARY  = 999
where EMP_CODE = 'EMP01' ;



--27) To check SCD Transformation in EMP_DTS_TRG  update records having EMP_SALARY is Greater than 1000



Update EMP_DTS_SRC1 set EMP_SALARY  = 1001
where EMP_CODE = 'EMP01' ;



--Check data Source and target table

Select * from EMP_DTS_SRC1;

Select * from EMP_DTS_TRG ; 


--28)  To check SCD Transformation in EMP_DTS_TRG  update records having EMP_SALARY is equal to  1000



Update EMP_DTS_SRC1 set EMP_SALARY  = 1000
where EMP_CODE = 'EMP01' ;



--Check data Source and target table

Select * from EMP_DTS_SRC1;

Select * from EMP_DTS_TRG ;






--29) To validate SCD Transformatio in EMP_DTS_TRG  update record  in EMP_DTS_SRC1 when current year is equal to hire year.


--Delete all data in source and target table
Truncate table EMP_DTS_SRC1 ;
Truncate table EMP_DTS_SRC2 ;
Truncate table EMP_DTS_SRC3 ;
Truncate table EMP_DTS_TRG ;



--Insert test data in source table

INSERT INTO [dbo].[EMP_DTS_SRC1] ([EMP_ID],[LAST_NAME],[FIRST_NAME],[MIDDLE_NAME],[EMP_SALARY],[EMP_DOB],[EMP_GENDER],[HIRE_DATE])
     VALUES
           (1,'D','Rohit','S', '1000', '1993-11-01 00:00:00.000','Male','2017-03-31')
GO



--Check data Source and target table

Select * from EMP_DTS_SRC1;

Select * from EMP_DTS_TRG ; 


----Update salary in source table

Update EMP_DTS_SRC1 set HIRE_DATE  = '2021-03-31'
where EMP_CODE = 'EMP01' ;


----Update salary in source table

Update EMP_DTS_SRC1 set HIRE_DATE  = '2022-03-31'
where EMP_CODE = 'EMP01' ;



----Update salary in source table

Update EMP_DTS_SRC1 set HIRE_DATE  = '1920-03-31'
where EMP_CODE = 'EMP01' ;