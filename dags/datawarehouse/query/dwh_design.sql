DROP TABLE IF EXISTS data_payroll_champy_raw;
CREATE TABLE data_payroll_champy_raw (
    EmployeeID INT NOT NULL,
    Name VARCHAR(255),
    Gender VARCHAR(10),
    Age INT,
    Department VARCHAR(255),
    Position VARCHAR(255),
    Salary DECIMAL(15, 2),
    OvertimePay DECIMAL(15, 2), 
	PaymentDate DATE
);

DROP TABLE IF EXISTS data_performance_champy_raw;
CREATE TABLE data_performance_champy_raw (
    EmployeeID INT NOT NULL,
    Name VARCHAR(255),
    ReviewPeriod DATE,
    Rating INT,
    Comments TEXT
);

DROP TABLE IF EXISTS data_training_champy_raw;
CREATE TABLE data_training_champy_raw (
    EmployeeID INT NOT NULL,
    Name VARCHAR(255),
    TrainingProgram VARCHAR(255),
    StartDate DATE,
    EndDate DATE,
    Status VARCHAR(255)
);

DROP TABLE IF EXISTS data_recruitment_champy_raw;
CREATE TABLE data_recruitment_champy_raw (
    CandidateID INT NOT NULL,
    Name VARCHAR(255),
    Gender VARCHAR(10),
    Age INT,
    Position VARCHAR(255),
    ApplicationDate DATE,
    Status VARCHAR(255),
    InterviewDate DATE,
    OfferStatus VARCHAR(255)
);

-- Drop the fact_employee table and all dependent objects
-- DROP TABLE IF EXISTS fact_employee CASCADE;

-- Recreate fact_employee table
DROP TABLE IF EXISTS fact_employee;
CREATE TABLE fact_employee (
    "EmployeeID" INT PRIMARY KEY,
    "Name" VARCHAR(255),
    "Gender" VARCHAR(10),
    "Age" INT,
    "Department" VARCHAR(255),
    "Position" VARCHAR(255),
    "Salary" DECIMAL(15, 2),
    "OvertimePay" DECIMAL(15, 2)
);

-- -- Recreate dim_payroll table
-- DROP TABLE IF EXISTS dim_payroll;
-- CREATE TABLE dim_payroll (
--     "PayrollID" SERIAL PRIMARY KEY,
--     "EmployeeID" INT,
--     "PaymentDate" DATE,
--     FOREIGN KEY ("EmployeeID") REFERENCES fact_employee("EmployeeID")
-- );

-- ALTER TABLE dim_payroll ADD CONSTRAINT unique_employee_payment UNIQUE ("EmployeeID", "PaymentDate");

-- -- Recreate the dim_performance table
-- DROP TABLE IF EXISTS dim_performance;
-- CREATE TABLE dim_performance (
--     PerformanceID SERIAL PRIMARY KEY,
--     EmployeeID INT,
--     ReviewPeriod DATE,
--     Rating INT,
--     Comments TEXT,
--     Prediction VARCHAR(255),
--     FOREIGN KEY (EmployeeID) REFERENCES fact_employee(EmployeeID)
-- );

-- -- Recreate the dim_training table
-- DROP TABLE IF EXISTS dim_training;
-- CREATE TABLE dim_training (
--     TrainingID SERIAL PRIMARY KEY,
--     EmployeeID INT,
--     TrainingProgram VARCHAR(255),
--     StartDate DATE,
--     EndDate DATE,
--     Status VARCHAR(50),
--     FOREIGN KEY (EmployeeID) REFERENCES fact_employee(EmployeeID)
-- );

-- -- Recreate the dim_candidate table
-- DROP TABLE IF EXISTS dim_candidate;
-- CREATE TABLE dim_candidate (
--     CandidateID INT PRIMARY KEY,
--     Name VARCHAR(255),
--     Gender VARCHAR(10),
--     Age INT,
--     Position VARCHAR(255),
--     ApplicationDate DATE,
--     Status VARCHAR(50),
--     InterviewDate DATE,
--     OfferStatus VARCHAR(50),
--     Prediction VARCHAR(255)
-- );
