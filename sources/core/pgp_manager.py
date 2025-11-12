import subprocess
import os
import logging

logger = logging.getLogger(__name__)


class PGPManager:

    @staticmethod
    @staticmethod
    def decrypt_file(input_file: str,
                     output_file: str,
                     passphrase: str,
                     temp_keyring: str
                    ) -> None:
        """
        Decrypts a file using a specified GPG keyring (temp_keyring).
        If needed, you can import a private key into this keyring before calling decrypt.

        :param input_file: Path to the encrypted file.
        :param output_file: Path to save the decrypted file.
        :param passphrase: Passphrase for the private key used to decrypt.
        :param temp_keyring: Path to the temporary keyring file (non-default keyring).
        :param private_key_file: (Optional) If you have a separate step importing a private key
                                 file into temp_keyring, pass it here if needed for logging or checks.
        """
        try:
            # Construct the decryption command
            command = [
                "gpg",
                "--no-default-keyring",
                "--keyring", temp_keyring,
                "--batch",
                "--yes",
                "--pinentry-mode", "loopback",
                "--passphrase", passphrase,
                "--output", output_file,
                "--decrypt", input_file
            ]

            # Run the command
            subprocess.run(command, capture_output=True, text=True, check=True)
            logging.info(f"File decrypted successfully and saved to {output_file}")

        except subprocess.CalledProcessError as e:
            logging.error(f"GPG decryption failed: {e.stderr.strip()}")
            raise
        except Exception as e:
            logging.error(f"Unexpected error during decryption: {str(e)}")
            raise

    @staticmethod
    def gpg_name_for_output(input_file, pgp_output_directory, pgp_name_method, pgp_number_of_char_rem=None,
                            pgp_char_to_add=None):
        """
        Generate the output file name based on the specified PGP naming method.

        Parameters:
        - input_file (str): Path to the input file.
        - pgp_output_directory (str): Path to the output directory.
        - pgp_name_method (str): Method to modify the file name.
        - pgp_number_of_char_rem (int, optional): Number of characters to remove from the file name.
        - pgp_char_to_add (str, optional): Characters to add to the file name after modification.

        Returns:
        - str: Path to the output file.
        """
        try:
            # Validate pgp_number_of_char_rem
            if pgp_name_method in ["REMOVE_CHAR", "REMOVE_CHAR_ADD_CHAR"]:
                if not isinstance(pgp_number_of_char_rem, int):
                    pgp_number_of_char_rem=int(pgp_number_of_char_rem)

            # Extract the base name of the input file
            base_name = os.path.basename(input_file)

            # Handle different naming methods
            if pgp_name_method == "REMOVE_CHAR":
                base_name = base_name[:-pgp_number_of_char_rem]
                output_file = os.path.join(pgp_output_directory, base_name)

            elif pgp_name_method == "REMOVE_CHAR_ADD_CHAR":
                base_name = base_name[:-pgp_number_of_char_rem]
                output_file = os.path.join(pgp_output_directory, base_name + pgp_char_to_add)

            elif pgp_name_method == "ADD_CHAR":
                output_file = os.path.join(pgp_output_directory, base_name + pgp_char_to_add)

            else:
                raise ValueError("Invalid pgp_name_method specified.")

            return output_file

        except Exception as e:
            print(f"Error in gpg_name_for_output: {e}")
            return None

    @staticmethod
    def gpg_encrypt_file(input_file, output_file, recipient, temp_keyring,public_key_file=None,):
        """
        Encrypts a file using the GPG command-line tool. If the recipient's key does not exist in the keyring,
        it imports the key from the specified file.

        Parameters:
        - input_file (str): Path to the plaintext file to encrypt.
        - output_file (str): Path to save the encrypted file.
        - recipient (str): Email or key ID of the recipient.
        - public_key_file (str): Path to the public key file (optional, required if the key is missing).

        """
        try:
            os.makedirs(os.path.dirname(output_file), exist_ok=True)

            # Command to encrypt the file
            encrypt_command = [
                "gpg",
                "--no-default-keyring",
                "--keyring", temp_keyring,
                "--batch",
                "--no-tty",
                "--yes",
                "--trust-model", "always",  # Automatically trust the key
                "--output", output_file,
                "--encrypt",
                "--recipient", recipient,
                input_file
            ]
            logging.info(f"encrypt_command={encrypt_command}")
            # Run the encryption command
            subprocess.run(encrypt_command, capture_output=True, check=True)

            logging.info(f"File encrypted successfully and saved to {output_file}")

        except subprocess.CalledProcessError as e:
            logging.error(f"Subprocess error: {e.stderr.strip()}")
            raise

        except Exception as e:
            logging.error(f"General error during encryption: {str(e)}")
            raise

    @staticmethod
    def import_private_key_with_passphrase(private_key_file: str, passphrase: str, temp_keyring: str):
        """
        Imports a private key into a specified GPG keyring with a provided passphrase.

        :param private_key_file: Path to the private key file.
        :param passphrase: The passphrase for the private key.
        :param temp_keyring: The path to the temporary keyring file.
        """
        try:
            # Prepare the GPG command
            command = [
                "gpg",
                "--batch",
                "--yes",
                "--no-tty",
                "--no-default-keyring",
                "--keyring", temp_keyring,
                "--pinentry-mode", "loopback",
                "--passphrase-fd", "0",
                "--import", private_key_file
            ]

            # Run the command and pass the passphrase through stdin
            process = subprocess.Popen(
                command,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            stdout, stderr = process.communicate(input=passphrase)

            # Check for errors
            if process.returncode != 0:
                logging.error(f"Error importing private key: {stderr}")
            else:
                logging.info(f"Private key from {private_key_file} imported successfully.\n{stdout}")

        except Exception as e:
            logging.error(f"An error occurred while importing private key: {e}")
            raise

    @staticmethod
    def import_public_key_file(public_key_file,temp_keyring):  # Corrected parameter name
        try:
            import_command = ["gpg", "--batch", "--no-tty","--no-default-keyring","--keyring", temp_keyring,"--import", public_key_file]
            subprocess.run(import_command, capture_output=True, check=True)
            logging.info(f"Public key from {public_key_file} imported successfully.")
        except subprocess.CalledProcessError as e:
            logging.error(f"Subprocess error: {e.stderr.strip()}")
            raise
        except Exception as e:
            logging.error(f"Error during importing public key file: {str(e)}")
            raise
