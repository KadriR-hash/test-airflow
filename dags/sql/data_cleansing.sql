
UPDATE employees_temp
SET Company_Name = UPPER(Company_Name),
    Employee_Markme = UPPER(Employee_Markme),
    Description = UPPER(Description);
