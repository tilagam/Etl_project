USE [master]
GO
/****** Object:  Database [EMP_ETL_V1]    Script Date: 2/15/2020 1:49:30 PM ******/
CREATE DATABASE [EMP_ETL_V1] 
Go
USE [EMP_ETL_V1]
GO
/****** Object:  Table [dbo].[EMP_DTS_SRC1]    Script Date: 2/15/2020 1:49:30 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
 CREATE TABLE [dbo].[EMP_DTS_SRC1](
	[EMP_ID] [int]  NOT NULL primary key,
	[EMP_CODE]  AS ('EMP'+ RIGHT('0' + CAST([EMP_ID] AS VARCHAR(7)), 7)) PERSISTED,
	[LAST_NAME] [varchar](100) Not NULL,
	[FIRST_NAME] [varchar](100) Not NULL,
	[MIDDLE_NAME] [varchar](100) NULL,
	[EMP_SALARY] [decimal](10, 2) Not NULL,
	[EMP_DOB] [datetime] NULL,
	[EMP_GENDER] [varchar](10),
	[HIRE_DATE] [date] Not NULL,
	Check ([EMP_GENDER] in ('Male', 'Female', 'Unknown'))
) ON [PRIMARY]
GO
 
/****** Object:  Table [dbo].[EMP_DTS_SRC2]    Script Date: 2/15/2020 1:49:30 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[EMP_DTS_SRC2](
 
[EMP_ID] [int]  NOT NULL primary key,
	[EMP_CODE]  AS ('E'+ RIGHT('00' + CAST([EMP_ID] AS VARCHAR(7)), 7)) PERSISTED,
	[LAST_NAME] [varchar](100) Not NULL,
	[FIRST_NAME] [varchar](100) Not NULL,
	[MIDDLE_NAME] [varchar](100) NULL,
	[EMP_SALARY] [decimal](10, 2) Not NULL,
	[EMP_DOB] [datetime] NULL,
	[EMP_GENDER] [varchar](10) NULL,
	[HIRE_DATE] [date] Not NULL,
	Check ([EMP_GENDER] in ('M', 'F', 'NA'))
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[EMP_DTS_SRC3]    Script Date: 2/15/2020 1:49:30 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[EMP_DTS_SRC3](
[EMP_ID] [int]  NOT NULL primary key,
	[EMP_CODE]  AS ('ID'+ RIGHT('1' + CAST([EMP_ID] AS VARCHAR(7)), 7)) PERSISTED,
	[LAST_NAME] [varchar](100) Not NULL,
	[FIRST_NAME] [varchar](100) Not NULL,
	[MIDDLE_NAME] [varchar](100) NULL,
	[EMP_SALARY] [decimal](10, 2) Not NULL,
	[EMP_DOB] [datetime] NULL,
	[EMP_GENDER] [varchar](10) NULL,
	[HIRE_DATE] [date] Not NULL,
	Check ([EMP_GENDER] in ('1', '2', 'Unknown'))
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[EMP_DTS_TRG]    Script Date: 2/15/2020 1:49:30 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[EMP_DTS_TRG](
	[ID] [int] IDENTITY(1,1) NOT NULL primary key,
	[EMP_CODE] [varchar](100) Not Null,
	[EMP_NAME] [varchar](100) NULL,
	[EMP_SALARY] [decimal](10, 2) NULL,
	[EMP_DOB] [datetime] NULL,
	[EMP_GENDER] [varchar](10) NULL,
	[HIRE_DATE] [datetime] NULL,
	[TOTAL_EXP] [varchar](2) NULL,
	[REFERENCE_TABLE] [varchar](100) NULL,
	[TIMESTAMP] [datetime] NULL
) ON [PRIMARY]
GO

USE [EMP_ETL_V1]
GO
/****** Object:  Trigger [dbo].[trgAfterInsert1]    Script Date: 2/15/2020 1:49:30 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
Create TRIGGER [dbo].[trgAfterInsert1] ON [dbo].[EMP_DTS_SRC1] 
FOR INSERT
AS
	declare @EMP_CODE VARCHAR (100);
	declare @LAST_NAME varchar(100);
	declare @FIRST_NAME varchar(100);
	declare @MIDDLE_NAME varchar(100);	
	declare @EMPSALARY decimal(10,2);
	declare @EMP_DOB DATETIME;
	declare @EMP_GENDER Varchar(10);
	declare @HIRE_DATE DATETIME;
	declare @TOTAL_EXP Varchar(10);		
	declare @REFERENCE_TABLE varchar(100);
	

	select @EMP_CODE=i.EMP_CODE from inserted i;	
	select @EMPSALARY=i.EMP_SALARY from inserted i;
	select @EMP_DOB=i.EMP_SALARY from inserted i;
	select @EMP_GENDER=i.EMP_GENDER from inserted i;
	select @HIRE_DATE=i.HIRE_DATE from inserted i;
	select @LAST_NAME=i.LAST_NAME from inserted i;
	select @FIRST_NAME=i.FIRST_NAME from inserted i;
	select @MIDDLE_NAME=i.MIDDLE_NAME from inserted i;
	select @EMP_GENDER=CASE WHEN i.EMP_GENDER='1' OR (i.EMP_GENDER)='male' 
	OR (i.EMP_GENDER)='M' THEN 'M' WHEN i.EMP_GENDER='2' OR (i.EMP_GENDER)='female' THEN 'F' END from inserted i;	
	set @REFERENCE_TABLE='EMP_DTS_SRC1';
	set @TOTAL_EXP='EMP_DTS_SRC1';		
	set @REFERENCE_TABLE='EMP_DTS_SRC1';
	IF(@EMPSALARY>=1000)
	begin
	

	insert into EMP_DTS_TRG
           (EMP_CODE,EMP_NAME,EMP_SALARY,EMP_DOB,EMP_GENDER,HIRE_DATE,TOTAL_EXP,REFERENCE_TABLE, TIMESTAMP) 
	values(@EMP_CODE,(@LAST_NAME + ' ' + @FIRST_NAME + ' ' + @MIDDLE_NAME) ,@EMPSALARY,@EMP_DOB,@EMP_GENDER,
	@HIRE_DATE,DATEDIFF (YY,@HIRE_DATE,GETDATE()), @REFERENCE_TABLE,getdate());
END
	PRINT 'New data loaded at EMP_DTS_TRG from EMP_DTS_SRC1'
GO
ALTER TABLE [dbo].[EMP_DTS_SRC1] ENABLE TRIGGER [trgAfterInsert1]
GO
/****** Object:  Trigger [dbo].[trgAfterUPDATE1]    Script Date: 2/15/2020 1:49:30 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
USE [EMP_ETL_V1]
GO
Create TRIGGER [dbo].[trgAfterUPDATE1] ON [dbo].[EMP_DTS_SRC1] 
FOR UPDATE
AS
	declare @EMP_CODE VARCHAR (100);
	declare @LAST_NAME varchar(100);
	declare @FIRST_NAME varchar(100);
	declare @MIDDLE_NAME varchar(100);	
	declare @EMPSALARY decimal(10,2);
	declare @EMP_DOB DATETIME;
	declare @EMP_GENDER Varchar(10);
	declare @HIRE_DATE DATETIME;
	declare @TOTAL_EXP Varchar(10);		
	declare @REFERENCE_TABLE varchar(100);
	

	select @EMP_CODE=i.EMP_CODE from inserted i;	
	select @EMPSALARY=i.EMP_SALARY from inserted i;
	select @EMP_DOB=i.EMP_SALARY from inserted i;
	select @EMP_GENDER=i.EMP_GENDER from inserted i;
	select @HIRE_DATE=i.HIRE_DATE from inserted i;
	select @LAST_NAME=i.LAST_NAME from inserted i;
	select @FIRST_NAME=i.FIRST_NAME from inserted i;
	select @MIDDLE_NAME=i.MIDDLE_NAME from inserted i;
	select @EMP_GENDER=CASE WHEN i.EMP_GENDER='1' OR (i.EMP_GENDER)='male' 
	OR (i.EMP_GENDER)='M' THEN 'M' WHEN i.EMP_GENDER='2' OR (i.EMP_GENDER)='female' THEN 'F' END from inserted i;	
	set @REFERENCE_TABLE='EMP_DTS_SRC1';
	set @TOTAL_EXP='EMP_DTS_SRC1';		
	set @REFERENCE_TABLE='EMP_DTS_SRC1';
	IF(@EMPSALARY>=1000)
	begin
	

	insert into EMP_DTS_TRG
           (EMP_CODE,EMP_NAME,EMP_SALARY,EMP_DOB,EMP_GENDER,HIRE_DATE,TOTAL_EXP,REFERENCE_TABLE, TIMESTAMP) 
	values(@EMP_CODE,(@LAST_NAME + ' ' + @FIRST_NAME + ' ' + @MIDDLE_NAME) ,@EMPSALARY,@EMP_DOB,@EMP_GENDER,
	@HIRE_DATE,DATEDIFF (YY,@HIRE_DATE,GETDATE()), @REFERENCE_TABLE,getdate());
END
	PRINT 'Modified data loaded at EMP_DTS_TRG from EMP_DTS_SRC1'
GO
ALTER TABLE [dbo].[EMP_DTS_SRC1] ENABLE TRIGGER [trgAfterUPDATE1]
GO
/****** Object:  Trigger [dbo].[trgAfterInsert2]    Script Date: 2/15/2020 1:49:30 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
Create TRIGGER [dbo].[trgAfterInsert2] ON [dbo].[EMP_DTS_SRC2] 
FOR INSERT
AS
	declare @EMP_CODE VARCHAR (100);
	declare @LAST_NAME varchar(100);
	declare @FIRST_NAME varchar(100);
	declare @MIDDLE_NAME varchar(100);	
	declare @EMPSALARY decimal(10,2);
	declare @EMP_DOB DATETIME;
	declare @EMP_GENDER Varchar(10);
	declare @HIRE_DATE DATETIME;
	declare @TOTAL_EXP Varchar(10);		
	declare @REFERENCE_TABLE varchar(100);
	

	select @EMP_CODE=i.EMP_CODE from inserted i;	
	select @EMPSALARY=i.EMP_SALARY from inserted i;
	select @EMP_DOB=i.EMP_SALARY from inserted i;
	select @EMP_GENDER=i.EMP_GENDER from inserted i;
	select @HIRE_DATE=i.HIRE_DATE from inserted i;
	select @LAST_NAME=i.LAST_NAME from inserted i;
	select @FIRST_NAME=i.FIRST_NAME from inserted i;
	select @MIDDLE_NAME=i.MIDDLE_NAME from inserted i;
	select @EMP_GENDER=CASE WHEN i.EMP_GENDER='1' OR (i.EMP_GENDER)='male' 
	OR (i.EMP_GENDER)='M' THEN 'M' WHEN i.EMP_GENDER='2' OR (i.EMP_GENDER)='female' THEN 'F' END from inserted i;	
	set @REFERENCE_TABLE='EMP_DTS_SRC2';
	set @TOTAL_EXP='EMP_DTS_SRC2';		
	set @REFERENCE_TABLE='EMP_DTS_SRC2';
	IF(@EMPSALARY>=1000)
	begin
	

	insert into EMP_DTS_TRG
           (EMP_CODE,EMP_NAME,EMP_SALARY,EMP_DOB,EMP_GENDER,HIRE_DATE,TOTAL_EXP,REFERENCE_TABLE, TIMESTAMP) 
	values(@EMP_CODE,(@LAST_NAME + ' ' + @FIRST_NAME + ' ' + @MIDDLE_NAME) ,@EMPSALARY,@EMP_DOB,@EMP_GENDER,
	@HIRE_DATE,DATEDIFF (YY,@HIRE_DATE,GETDATE()), @REFERENCE_TABLE,getdate());
END
	PRINT 'New data loaded at EMP_DTS_TRG from EMP_DTS_SRC2'
GO
ALTER TABLE [dbo].[EMP_DTS_SRC2] ENABLE TRIGGER [trgAfterInsert2]
GO
/****** Object:  Trigger [dbo].[trgAfterUPDATE2]    Script Date: 2/15/2020 1:49:30 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
USE [EMP_ETL_V1]
GO

CREATE  TRIGGER [dbo].[trgAfterUPDATE2] ON [dbo].[EMP_DTS_SRC2] 
FOR UPDATE
AS
	declare @EMP_CODE VARCHAR (100);
	declare @LAST_NAME varchar(100);
	declare @FIRST_NAME varchar(100);
	declare @MIDDLE_NAME varchar(100);	
	declare @EMPSALARY decimal(10,2);
	declare @EMP_DOB DATETIME;
	declare @EMP_GENDER Varchar(10);
	declare @HIRE_DATE DATETIME;
	declare @TOTAL_EXP Varchar(10);		
	declare @REFERENCE_TABLE varchar(100);
	

	select @EMP_CODE=i.EMP_CODE from inserted i;	
	select @EMPSALARY=i.EMP_SALARY from inserted i;
	select @EMP_DOB=i.EMP_SALARY from inserted i;
	select @EMP_GENDER=i.EMP_GENDER from inserted i;
	select @HIRE_DATE=i.HIRE_DATE from inserted i;
	select @LAST_NAME=i.LAST_NAME from inserted i;
	select @FIRST_NAME=i.FIRST_NAME from inserted i;
	select @MIDDLE_NAME=i.MIDDLE_NAME from inserted i;
	select @EMP_GENDER=CASE WHEN i.EMP_GENDER='1' OR (i.EMP_GENDER)='male' 
	OR (i.EMP_GENDER)='M' THEN 'M' WHEN i.EMP_GENDER='2' OR (i.EMP_GENDER)='female' THEN 'F' END from inserted i;	
	set @REFERENCE_TABLE='EMP_DTS_SRC2';
	set @TOTAL_EXP='EMP_DTS_SRC2';		
	set @REFERENCE_TABLE='EMP_DTS_SRC2';
	IF(@EMPSALARY>=1000)
	begin
	

	insert into EMP_DTS_TRG
           (EMP_CODE,EMP_NAME,EMP_SALARY,EMP_DOB,EMP_GENDER,HIRE_DATE,TOTAL_EXP,REFERENCE_TABLE, TIMESTAMP) 
	values(@EMP_CODE,(@LAST_NAME + ' ' + @FIRST_NAME + ' ' + @MIDDLE_NAME) ,@EMPSALARY,@EMP_DOB,@EMP_GENDER,
	@HIRE_DATE,DATEDIFF (YY,@HIRE_DATE,GETDATE()), @REFERENCE_TABLE,getdate());
END
PRINT 'Modified data loaded at EMP_DTS_TRG from EMP_DTS_SRC2'
GO
ALTER TABLE [dbo].[EMP_DTS_SRC2] ENABLE TRIGGER [trgAfterUPDATE2]
GO
/****** Object:  Trigger [dbo].[trgAfterInsert3]    Script Date: 2/15/2020 1:49:30 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
Create TRIGGER [dbo].[trgAfterInsert3] ON [dbo].[EMP_DTS_SRC3] 
FOR INSERT
AS
	declare @EMP_CODE VARCHAR (100);
	declare @LAST_NAME varchar(100);
	declare @FIRST_NAME varchar(100);
	declare @MIDDLE_NAME varchar(100);	
	declare @EMPSALARY decimal(10,2);
	declare @EMP_DOB DATETIME;
	declare @EMP_GENDER Varchar(10);
	declare @HIRE_DATE DATETIME;
	declare @TOTAL_EXP Varchar(10);		
	declare @REFERENCE_TABLE varchar(100);
	

	select @EMP_CODE=i.EMP_CODE from inserted i;	
	select @EMPSALARY=i.EMP_SALARY from inserted i;
	select @EMP_DOB=i.EMP_SALARY from inserted i;
	select @EMP_GENDER=i.EMP_GENDER from inserted i;
	select @HIRE_DATE=i.HIRE_DATE from inserted i;
	select @LAST_NAME=i.LAST_NAME from inserted i;
	select @FIRST_NAME=i.FIRST_NAME from inserted i;
	select @MIDDLE_NAME=i.MIDDLE_NAME from inserted i;
	select @EMP_GENDER=CASE WHEN i.EMP_GENDER='1' OR (i.EMP_GENDER)='male' 
	OR (i.EMP_GENDER)='M' THEN 'M' WHEN i.EMP_GENDER='2' OR (i.EMP_GENDER)='female' THEN 'F' END from inserted i;	
	set @REFERENCE_TABLE='EMP_DTS_SRC3';
	set @TOTAL_EXP='EMP_DTS_SRC3';		
	set @REFERENCE_TABLE='EMP_DTS_SRC3';
	IF(@EMPSALARY>=1000)
	begin
	insert into EMP_DTS_TRG
           (EMP_CODE,EMP_NAME,EMP_SALARY,EMP_DOB,EMP_GENDER,HIRE_DATE,TOTAL_EXP,REFERENCE_TABLE, TIMESTAMP) 
	values(@EMP_CODE,(@LAST_NAME + ' ' + @FIRST_NAME + ' ' + @MIDDLE_NAME) ,@EMPSALARY,@EMP_DOB,@EMP_GENDER,
	@HIRE_DATE,DATEDIFF (YY,@HIRE_DATE,GETDATE()), @REFERENCE_TABLE,getdate());
END
PRINT 'New data loaded at EMP_DTS_TRG from EMP_DTS_SRC3'
GO
ALTER TABLE [dbo].[EMP_DTS_SRC3] ENABLE TRIGGER [trgAfterInsert3]
GO
/****** Object:  Trigger [dbo].[trgAfterUPDATE3]    Script Date: 2/15/2020 1:49:30 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

Create TRIGGER [dbo].[trgAfterUPDATE3] ON [dbo].[EMP_DTS_SRC3] 
FOR UPDATE
AS
	declare @EMP_CODE VARCHAR (100);
	declare @LAST_NAME varchar(100);
	declare @FIRST_NAME varchar(100);
	declare @MIDDLE_NAME varchar(100);	
	declare @EMPSALARY decimal(10,2);
	declare @EMP_DOB DATETIME;
	declare @EMP_GENDER Varchar(10);
	declare @HIRE_DATE DATETIME;
	declare @TOTAL_EXP Varchar(10);		
	declare @REFERENCE_TABLE varchar(100);
	

	select @EMP_CODE=i.EMP_CODE from inserted i;	
	select @EMPSALARY=i.EMP_SALARY from inserted i;
	select @EMP_DOB=i.EMP_SALARY from inserted i;
	select @EMP_GENDER=i.EMP_GENDER from inserted i;
	select @HIRE_DATE=i.HIRE_DATE from inserted i;
	select @LAST_NAME=i.LAST_NAME from inserted i;
	select @FIRST_NAME=i.FIRST_NAME from inserted i;
	select @MIDDLE_NAME=i.MIDDLE_NAME from inserted i;
	select @EMP_GENDER=CASE WHEN i.EMP_GENDER='1' OR (i.EMP_GENDER)='male' 
	OR (i.EMP_GENDER)='M' THEN 'M' WHEN i.EMP_GENDER='2' OR (i.EMP_GENDER)='female' THEN 'F' END from inserted i;	
	set @REFERENCE_TABLE='EMP_DTS_SRC3';
	set @TOTAL_EXP='EMP_DTS_SRC3';		
	set @REFERENCE_TABLE='EMP_DTS_SRC3';
	IF(@EMPSALARY>=1000)
	begin
	

	insert into EMP_DTS_TRG
           (EMP_CODE,EMP_NAME,EMP_SALARY,EMP_DOB,EMP_GENDER,HIRE_DATE,TOTAL_EXP,REFERENCE_TABLE, TIMESTAMP) 
	values(@EMP_CODE,(@LAST_NAME + ' ' + @FIRST_NAME + ' ' + @MIDDLE_NAME) ,@EMPSALARY,@EMP_DOB,@EMP_GENDER,
	@HIRE_DATE,DATEDIFF (YY,@HIRE_DATE,GETDATE()), @REFERENCE_TABLE,getdate());
END
	PRINT 'Modified data loaded at EMP_DTS_TRG from EMP_DTS_SRC3'
GO
ALTER TABLE [dbo].[EMP_DTS_SRC3] ENABLE TRIGGER [trgAfterUPDATE3]
GO
USE [master]
GO