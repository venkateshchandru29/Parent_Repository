CREATE OR REPLACE PROCEDURE BulkClaimsValidation(clm_id in number)
AS
    CURSOR claim_cursor IS
        SELECT * FROM claim_flow where claimid =clm_id;

    v_claim_id NUMBER;
    v_pat_id NUMBER;
    v_pro_id NUMBER;
    v_pol_id NUMBER;
    v_dos DATE;
    v_amt NUMBER;
    v_dx_code VARCHAR2(50);
    v_cpt_code NUMBER;
    v_status VARCHAR2(50);
    v_f_name VARCHAR2(255);
    v_l_name VARCHAR2(255);
    v_dob DATE;
    v_p_ph VARCHAR2(50);
    v_P_provider VARCHAR2(255);
    v_var patientinformation%ROWTYPE;
    p_dat DATE;
    p_pro providerinformation%ROWTYPE;
    p_num NUMBER(10);
    p_code NUMBER(20);
    d_code NUMBER(20);
    p_bal NUMBER(20);
    v_claim_count NUMBER(10);

BEGIN

    SELECT COUNT(*) INTO v_claim_count FROM claim_flow WHERE claimid = clm_id;

    IF v_claim_count = 0 THEN
        RAISE_APPLICATION_ERROR(-20001, 'Claim ID does not exist in claim_flow.');
    END IF;

    OPEN claim_cursor;
    LOOP
        FETCH claim_cursor INTO v_claim_id, v_pat_id, v_pro_id, v_pol_id, v_dos, v_amt, v_dx_code, v_cpt_code, v_status, v_f_name, v_l_name,v_p_ph,v_P_provider;
        EXIT WHEN claim_cursor%NOTFOUND;

        BEGIN
            -- Patient validation
            SELECT * INTO v_var FROM patientinformation WHERE patientid = v_pat_id;
            
            IF v_var.firstname != v_f_name OR v_var.lastname != v_l_name THEN
                INSERT INTO claim_rejection_history
                VALUES (v_claim_id, v_pat_id, v_pro_id, v_pol_id, v_dos, v_amt, v_dx_code, v_cpt_code, v_status, v_f_name, v_l_name,v_p_ph, v_P_provider, 'Missing/invalid first or last name', SYSDATE);
                UPDATE claim_rejection_history SET claim_status = 'Error' WHERE claim_id = v_claim_id;
                dbms_output.put_line('Missing/invalid first or last name');
                CONTINUE;               
            END IF;

            IF v_var.dateofbirth != v_dob THEN
                INSERT INTO claim_rejection_history
                VALUES (v_claim_id, v_pat_id, v_pro_id, v_pol_id, v_dos, v_amt, v_dx_code, v_cpt_code, v_status, v_f_name, v_l_name, v_p_ph, v_P_provider, 'Missing/invalid DOB', SYSDATE);
                UPDATE claim_rejection_history SET claim_status = 'Error' WHERE claim_id = v_claim_id;
                dbms_output.put_line('Missing/invalid DOB');
                CONTINUE;
            ELSIF v_var.contactnumber != v_p_ph THEN
                INSERT INTO claim_rejection_history
                VALUES (v_claim_id, v_pat_id, v_pro_id, v_pol_id, v_dos, v_amt, v_dx_code, v_cpt_code, v_status, v_f_name, v_l_name, v_p_ph, v_P_provider, 'Missing/invalid Ph#', SYSDATE);
                UPDATE claim_rejection_history SET claim_status = 'Error' WHERE claim_id = v_claim_id;
                dbms_output.put_line('Missing/invalid Ph#');
                CONTINUE;
            END IF;

            -- Policy validation
            SELECT enddate INTO p_dat FROM policyinformation WHERE policyid = v_pol_id;
            IF v_dos > p_dat THEN
                INSERT INTO claim_rejection_history
                VALUES (v_claim_id, v_pat_id, v_pro_id, v_pol_id, v_dos, v_amt, v_dx_code, v_cpt_code, v_status, v_f_name, v_l_name,v_p_ph, v_P_provider, 'Patient not active on DOS', SYSDATE);
                UPDATE claim_rejection_history SET claim_status = 'Error' WHERE claim_id = v_claim_id;
                dbms_output.put_line('Patient not active');
                CONTINUE;
                
            END IF;

            -- Provider validation
            SELECT count(*) INTO p_num FROM providerinformation WHERE providerid = v_pro_id;
            IF p_num = 0 THEN
                INSERT INTO claim_rejection_history
                VALUES (v_claim_id, v_pat_id, v_pro_id, v_pol_id, v_dos, v_amt, v_dx_code, v_cpt_code, v_status, v_f_name, v_l_name, v_p_ph, v_P_provider, 'Invalid provider ID entered', SYSDATE);
                UPDATE claim_rejection_history SET claim_status = 'Error' WHERE claim_id = v_claim_id;
                 dbms_output.put_line('Provider ID is invalid');
                CONTINUE;
            END IF;

            SELECT * INTO p_pro FROM providerinformation WHERE providerid = v_pro_id;
            IF p_pro.providername != v_P_provider THEN
                INSERT INTO claim_rejection_history
                VALUES (v_claim_id, v_pat_id, v_pro_id, v_pol_id, v_dos, v_amt, v_dx_code, v_cpt_code, v_status, v_f_name, v_l_name, v_p_ph, v_P_provider, 'Missing/invalid provider name', SYSDATE);
                UPDATE claim_rejection_history SET claim_status = 'Error' WHERE claim_id = v_claim_id;
                dbms_output.put_line('Provider name is invalid ');
                CONTINUE;
            END IF;

            -- CPT and DX code validation
            SELECT count(*) INTO p_code FROM CPTDXCodeInformation WHERE code = TO_CHAR(v_cpt_code);
            SELECT count(*) INTO d_code FROM CPTDXCodeInformation WHERE code = v_dx_code;
            IF p_code = 0 OR d_code = 0 THEN
                INSERT INTO claim_rejection_history
                VALUES (v_claim_id, v_pat_id, v_pro_id, v_pol_id, v_dos, v_amt, v_dx_code, v_cpt_code, v_status, v_f_name, v_l_name, v_p_ph, v_P_provider, 'Missing/invalid CPT or DX code entered', SYSDATE);
                UPDATE claim_rejection_history SET claim_status = 'Error' WHERE claim_id = v_claim_id;
                dbms_output.put_line('Codes are incorrect');
                CONTINUE;
            END IF;

            -- Coverage balance check
            SELECT balance_amount INTO p_bal FROM patient_claim_summary WHERE patientid = v_pat_id;
            IF p_bal < v_amt THEN
                INSERT INTO claim_rejection_history
                VALUES (v_claim_id, v_pat_id, v_pro_id, v_pol_id, v_dos, v_amt, v_dx_code, v_cpt_code, v_status, v_f_name, v_l_name, v_p_ph, v_P_provider, 'Patients coverage amount met already', SYSDATE);
                UPDATE claim_rejection_history SET claim_status = 'Error' WHERE claim_id = v_claim_id;
                CONTINUE;
            ELSE
                INSERT INTO claim_flow VALUES (v_claim_id, v_pat_id, v_pro_id, v_pol_id, v_dos, v_amt, v_dx_code, v_cpt_code, v_status, v_f_name, v_l_name,v_p_ph, v_P_provider);
                UPDATE claims SET claimstatus = 'In progress' WHERE claimid = v_claim_id;
            END IF;

        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                INSERT INTO claim_rejection_history
                VALUES (v_claim_id, v_pat_id, v_pro_id, v_pol_id, v_dos, v_amt, v_dx_code, v_cpt_code, v_status, v_f_name, v_l_name, v_p_ph, v_P_provider, 'No data found', SYSDATE);
                UPDATE claim_rejection_history SET claim_status = 'Error' WHERE claim_id = v_claim_id;
                dbms_output.put_line('No data available');
            WHEN OTHERS THEN
                INSERT INTO claim_rejection_history
                VALUES (v_claim_id, v_pat_id, v_pro_id, v_pol_id, v_dos, v_amt, v_dx_code, v_cpt_code, v_status, v_f_name, v_l_name, v_p_ph, v_P_provider, 'Validation error', SYSDATE);
                UPDATE claim_rejection_history SET claim_status = 'Error' WHERE claim_id = v_claim_id;
                dbms_output.put_line('Validation error');
        END;
    END LOOP;

    CLOSE claim_cursor;
    commit;
END;