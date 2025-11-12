from core import ms_sql_bcp_manager
import logging
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    # Define parameters
    server = "JWSTGDB04.classic.pchad.com"
    database = "DIServicesMetaData_dev"
    table = "TalendDataSetTypes"
    user = "AIRFLOW_TEST"
    password = "airflow123"
    output_format_file = "/home/d_di_ms_talend_svc/keys/file_format.txt"

    # Generate the BCP format file
    success = ms_sql_bcp_manager.MsSqlBcpManager.generate_bcp_format_file(
        server=server,
        database=database,
        table=table,
        user=user,
        password=password,
        output_format_file=output_format_file
    )

    if success:
        print(f"Format file generated successfully: {output_format_file}")
    else:
        print("Failed to generate format file.")