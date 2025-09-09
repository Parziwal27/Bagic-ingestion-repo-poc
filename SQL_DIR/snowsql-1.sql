CREATE OR REPLACE PROCEDURE demo14444("INPUT" VARCHAR(16777216))
RETURNS VARCHAR(1433216)
LANGUAGE SQL
XECUTE AS CALLER
AS '
DECLARE out VARCHAR;
BEGIN
    -- using for tracking
    out := ''Your Input parameter from  : '' || input;
    RETURN out;
END;
';

