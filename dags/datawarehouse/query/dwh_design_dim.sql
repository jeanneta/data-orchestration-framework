ALTER TABLE fact_employee ADD CONSTRAINT unique_employee_id UNIQUE ("EmployeeID");


-- Recreate the dim_payroll table
DROP TABLE IF EXISTS dim_payroll;
CREATE TABLE dim_payroll (
    "PayrollID" SERIAL PRIMARY KEY,
    "EmployeeID" INT,
    "PaymentDate" DATE,
    FOREIGN KEY ("EmployeeID") REFERENCES fact_employee("EmployeeID")
);
--ALTER TABLE dim_payroll ADD CONSTRAINT unique_employee_payment UNIQUE ("EmployeeID", "PaymentDate");


-- Recreate the dim_performance table
DROP TABLE IF EXISTS dim_performance;
CREATE TABLE dim_performance (
    "PerformanceID" SERIAL PRIMARY KEY,
    "EmployeeID" INT,
    "ReviewPeriod" VARCHAR(255),
    "Rating" DECIMAL(3, 2),
    "Comments" TEXT,
    "Prediction" VARCHAR(255),
    FOREIGN KEY ("EmployeeID") REFERENCES fact_employee("EmployeeID") ON DELETE CASCADE
);


-- Recreate the dim_training table
DROP TABLE IF EXISTS dim_training;
CREATE TABLE dim_training (
    "TrainingID" SERIAL PRIMARY KEY,
    "EmployeeID" INT,
    "TrainingProgram" VARCHAR(255),
    "StartDate" DATE,
    "EndDate" DATE,
    "Status" VARCHAR(50),
    FOREIGN KEY ("EmployeeID") REFERENCES fact_employee("EmployeeID") ON DELETE CASCADE
);


-- Recreate the dim_candidate table
DROP TABLE IF EXISTS dim_candidate;
CREATE TABLE dim_candidate (
    "CandidateID" INT PRIMARY KEY,
    "Name" VARCHAR(255),
    "Gender" VARCHAR(10),
    "Age" INT,
    "Position" VARCHAR(255),
    "ApplicationDate" DATE,
    "Status" VARCHAR(50),
    "InterviewDate" DATE,
    "OfferStatus" VARCHAR(50),
    "Prediction" VARCHAR(255)
);


ALTER TABLE dim_candidate  ADD CONSTRAINT unique_candidate_id UNIQUE ("CandidateID");

ALTER TABLE dim_payroll  ADD CONSTRAINT unique_dim_payroll UNIQUE ("EmployeeID", "PaymentDate");

ALTER TABLE dim_performance  ADD CONSTRAINT unique_dim_performance UNIQUE ("EmployeeID", "ReviewPeriod");

ALTER TABLE dim_training  ADD CONSTRAINT unique_dim_training UNIQUE ("EmployeeID", "StartDate");
