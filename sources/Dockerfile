# Base image
FROM google/cloud-sdk:latest
LABEL authors="di_ms_talend_svc"

# Install required packages
RUN apt-get update && apt-get install -y \
    wget curl gnupg apt-transport-https \
    unixodbc-dev python3-pip python3-dev \
    default-mysql-client default-libmysqlclient-dev \
    build-essential pkg-config  \
    openssh-client sshpass \
 && rm -rf /var/lib/apt/lists/*



# Fix OpenSSL settings for lower TLS version compatibility (e.g. for SQL Server)
RUN sed -i 's/^MinProtocol\s*=.*/MinProtocol = TLSv1.2/' /etc/ssl/openssl.cnf || echo "MinProtocol = TLSv1.2" >> /etc/ssl/openssl.cnf && \
    sed -i 's/^CipherString\s*=.*/CipherString = DEFAULT@SECLEVEL=2/' /etc/ssl/openssl.cnf || echo "CipherString = DEFAULT@SECLEVEL=2" >> /etc/ssl/openssl.cnf

# Add Microsoft repository and install ODBC drivers for SQL Server
RUN wget -qO- https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor > microsoft.gpg && \
    install -o root -g root -m 644 microsoft.gpg /etc/apt/trusted.gpg.d/ && \
    rm microsoft.gpg && \
    wget -qO /etc/apt/sources.list.d/mssql-release.list https://packages.microsoft.com/config/ubuntu/20.04/prod.list && \
    apt-get update && \
    ACCEPT_EULA=Y apt-get install -y msodbcsql17 mssql-tools

# Add Microsoft tools to PATH
ENV PATH="/opt/mssql-tools/bin:${PATH}"

# Copy Python dependencies and install them
COPY requirements.txt ./
RUN pip install -r requirements.txt --break-system-packages

# Create directory structure
RUN mkdir -p "/home/source/core" "/home/source/db" \
    "/home/d_di_ms_talend_svc/gpgkeys" "/home/d_di_ms_talend_svc/data" \
    "/home/d_di_ms_talend_svc/keys"

# Copy code files
COPY core/* /home/source/core
COPY db/* /home/source/db
COPY /managers/__init__.py /home/source/__init__.py
COPY /managers/sftp_gs_manager/sftp_gs_manager.py /home/source/sftp_gs_manager.py
COPY /managers/gcs_sftp_manager/gcs_sftp_manager.py /home/source/gcs_sftp_manager.py
COPY /managers/bq_gs_manager/bq_gs_manager.py /home/source/bq_gs_manager.py
COPY /managers/gs_s3_manager/gcs_s3_manager.py /home/source/gcs_s3_manager.py
COPY /managers/gs_ms_sql_manager/gcs_ms_sql_manager.py /home/source/gcs_ms_sql_manager.py
COPY /managers/gs_ms_sql_manager/gcs_ms_sql_manager_client.py /home/source/gcs_ms_sql_manager_client.py
COPY /managers/ms_sql_to_gs_manager/ms_sql_gs_manager.py  /home/source/ms_sql_gs_manager.py
COPY /managers/ms_sql_statment_no_data_out_manager/ms_sql_no_data_out_manager.py /home/source/ms_sql_no_data_out_manager.py
COPY /managers/bq_statment_no_data_out_manager/bq_sql_manager_no_data_out.py /home/source/bq_sql_manager_no_data_out.py
COPY /managers/pgp_gs_file/pgp_gs_file_manager_encr.py /home/source/pgp_gs_file_manager_encr.py
COPY /managers/pgp_gs_file/pgp_gs_file_manager_decr.py /home/source/pgp_gs_file_manager_decr.py
COPY /managers/custom_uninty_api/custom_api_rest_unity.py  /home/source/custom_api_rest_unity.py
COPY /managers/mysql_to_gs_manager/mysql_sql_gs_manager.py  /home/source/mysql_sql_gs_manager.py
COPY /managers/bq_table_latency_alert_manager/bq_table_latency_alert_manager.py  /home/source/bq_table_latency_alert_manager.py
COPY /managers/bq_sent_html_table/bq_setn_hmtl_table.py /home/source/bq_setn_hmtl_table.py
COPY /managers/gs_bq_manager/gs_bq_manager.py /home/source/gs_bq_manager.py
COPY /managers/gs_mysql_manager/gs_mysql_manager.py /home/source/gs_mysql_manager.py
COPY /managers/mysql_statment_no_data_out_manager/mysql_no_data_out_manager.py /home/source/mysql_no_data_out_manager.py
COPY /managers/mysql_recon_json_custom/mysql_recon_json_custom.py /home/source/mysql_recon_json_custom.py
COPY /managers/insert_mysq_dataset_to_mssql_table/insert_mysql_dataset_to_mssql_table.py /home/source/insert_mysql_dataset_to_mssql_table.py
COPY /managers/insert_mssql_dataset_to_mssql_table/insert_mssql_dataset_to_mssql_table.py /home/source/insert_mssql_dataset_to_mssql_table.py
COPY /managers/insert_mssql_dataset_to_mysql_table/insert_mssql_dataset_to_mysql_table.py /home/source/insert_mssql_dataset_to_mysql_table.py
COPY /managers/bq_gs_manager/bq_gs_manager_simple.py /home/source/bq_gs_manager_simple.py
COPY /managers/bq_gs_manager/bq_gs_manager_with_split.py /home/source/bq_gs_manager_with_split.py
COPY /managers/app_flier_custom_manager/app_flier_custom_manager.py /home/source/app_flier_custom_manager.py
COPY /managers/recon_surveyprequals_custom/recon_surveyprequals_custom_manager.py /home/source/recon_surveyprequals_custom_manager.py
COPY /managers/compl_exhange_external/api_compl_exhange_external.py /home/source/api_compl_exhange_external.py
COPY /managers/ms_sql_to_gs_manager/ms_sql_gs_manager_PYmsql.py /home/source/ms_sql_gs_manager_PYmsql.py
COPY /managers/s3_to_gs_manager/s3_to_gs_manager.py /home/source/s3_to_gs_manager.py
COPY /managers/sent_file_via_email_manager/sent_file_via_email_manager.py /home/source/sent_file_via_email_manager.py
COPY /managers/max_user_ad_revenue_api_custom_manager/max_user_ad_revenue_api_custom_manager.py /home/source/max_user_ad_revenue_api_custom_manager.py


# Default CMD
CMD ["bash"]
