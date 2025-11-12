import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.utils import formatdate, make_msgid
from email import encoders
import mimetypes
import os
import logging


class EmailSender:
    def __init__(
        self,
        smtp_server: str,
        smtp_port: int,
        login_email: str,          # <-- SMTP auth username
        login_password: str,       # <-- SMTP auth password
        *,
        use_tls: bool = True,
        use_ssl: bool = False,
        default_from: str | None = None,  # header From default; falls back to login_email
    ):
        self.smtp_server = smtp_server
        self.smtp_port = smtp_port
        self.login_email = login_email
        self.login_password = login_password
        self.use_tls = use_tls
        self.use_ssl = use_ssl
        self.default_from = default_from or login_email

    @staticmethod
    def _split_csv(value: str):
        return [v.strip() for v in value.split(",") if v.strip()] if value else []

    def send_email(
        self,
        recipient_email: str,
        subject: str,
        body: str,
        is_html: str = "N",
        *,
        from_email: str | None = None,    # <-- header From (can differ from login_email)
        envelope_from: str | None = None, # <-- SMTP MAIL FROM / return-path
        cc: str = "",
        bcc: str = "",
        reply_to: str = "",
        attachments: str = "",
    ):
        """
        Send an email (plain/HTML) with optional cc/bcc and attachments.

        - Authenticate with self.login_email / self.login_password.
        - 'from_email' controls the visible From header.
        - 'envelope_from' controls the SMTP MAIL FROM (bounce address). Defaults to login_email.
        """
        to_list = self._split_csv(recipient_email)
        cc_list = self._split_csv(cc)
        bcc_list = self._split_csv(bcc)
        all_rcpts = list(dict.fromkeys(to_list + cc_list + bcc_list))  # dedupe

        if not all_rcpts:
            raise ValueError("No recipients provided.")

        from_email = from_email or self.default_from
        envelope_from = envelope_from or self.login_email

        msg = MIMEMultipart()
        msg["Subject"] = subject
        msg["From"] = from_email
        msg["To"] = ", ".join(to_list)
        if cc_list:
            msg["Cc"] = ", ".join(cc_list)
        if reply_to:
            msg["Reply-To"] = reply_to
        # If header From differs from authenticated sender, set Sender for transparency
        if from_email.lower() != self.login_email.lower():
            msg["Sender"] = self.login_email

        msg["Date"] = formatdate(localtime=True)
        msg["Message-ID"] = make_msgid()

        mime_type = "html" if is_html.upper() == "Y" else "plain"
        msg.attach(MIMEText(body, mime_type, _charset="utf-8"))

        # Attach files
        for path in self._split_csv(attachments):
            if not os.path.isfile(path):
                raise FileNotFoundError(f"Attachment not found: {path}")

            ctype, encoding = mimetypes.guess_type(path)
            if ctype is None or encoding is not None:
                ctype = "application/octet-stream"
            maintype, subtype = ctype.split("/", 1)

            with open(path, "rb") as f:
                part = MIMEBase(maintype, subtype)
                part.set_payload(f.read())
                encoders.encode_base64(part)

            part.add_header("Content-Disposition", "attachment", filename=os.path.basename(path))
            msg.attach(part)

        # Send
        if self.use_ssl:
            smtp_cls = smtplib.SMTP_SSL
        else:
            smtp_cls = smtplib.SMTP

        with smtp_cls(self.smtp_server, self.smtp_port) as server:
            if self.use_tls and not self.use_ssl:
                server.starttls()
            server.login(self.login_email, self.login_password)
            server.sendmail(envelope_from, all_rcpts, msg.as_string())

        logging.info(
            "Email sent. from=%s (login=%s) to=%s%s%s",
            from_email,
            self.login_email,
            ", ".join(to_list),
            f" | cc: {', '.join(cc_list)}" if cc_list else "",
            f" | bcc: {', '.join(bcc_list)}" if bcc_list else "",
        )
