-- MERGE statement for Clients table (CORRECTED)
MERGE final_clients AS target
USING raw_clients_staging AS source
ON (target.client_id = source.client_id)
WHEN MATCHED THEN
    UPDATE SET
        target.name = source.name,
        target.email = source.email,
        target.phone = source.phone,
        target.updated_at = source.updated_at,
        target.deleted_at = CASE
                                WHEN source._operation = 'DELETE' THEN source.deleted_at
                                WHEN source._operation = 'UPSERT' THEN NULL
                                ELSE target.deleted_at
                            END
WHEN NOT MATCHED BY TARGET AND source._operation = 'UPSERT' THEN
    INSERT (client_id, name, email, phone, created_at, updated_at, deleted_at)
    VALUES (source.client_id, source.name, source.email, source.phone, source.created_at, source.updated_at, source.deleted_at);

-- After successful merge, update the watermark for clients
DECLARE @max_updated_at_clients DATETIME;
SELECT @max_updated_at_clients = MAX(updated_at) FROM raw_clients_staging;
IF @max_updated_at_clients IS NOT NULL
BEGIN
    UPDATE cdc_watermarks
    SET last_processed_timestamp = @max_updated_at_clients
    WHERE table_name = 'clients';
END;

-- Clear raw staging table after processing
TRUNCATE TABLE raw_clients_staging;

-- MERGE statement for Appointments table (CORRECTED)
MERGE final_appointments AS target
USING raw_appointments_staging AS source
ON (target.appointment_id = source.appointment_id)
WHEN MATCHED THEN
    UPDATE SET
        target.client_id = source.client_id,
        target.service = source.service,
        target.appointment_time = source.appointment_time,
        target.status = source.status,
        target.updated_at = source.updated_at,
        target.deleted_at = CASE
                                WHEN source._operation = 'DELETE' THEN source.deleted_at
                                WHEN source._operation = 'UPSERT' THEN NULL
                                ELSE target.deleted_at
                            END
WHEN NOT MATCHED BY TARGET AND source._operation = 'UPSERT' THEN
    INSERT (appointment_id, client_id, service, appointment_time, status, created_at, updated_at, deleted_at)
    VALUES (source.appointment_id, source.client_id, source.service, source.appointment_time, source.status, source.created_at, source.updated_at, source.deleted_at);

-- After successful merge, update the watermark for appointments
DECLARE @max_updated_at_appointments DATETIME;
SELECT @max_updated_at_appointments = MAX(updated_at) FROM raw_appointments_staging;
IF @max_updated_at_appointments IS NOT NULL
BEGIN
    UPDATE cdc_watermarks
    SET last_processed_timestamp = @max_updated_at_appointments
    WHERE table_name = 'appointments';
END;

-- Clear raw staging table after processing
TRUNCATE TABLE raw_appointments_staging;