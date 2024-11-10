import os
import bz2
import re

def shorten_bz2_files(input_path, output_dir='shorts', num_rows=10):
    """
    Extracts the first INSERT INTO statement with limited rows and retains the initial structure
    from .bz2 files. Only 'num_rows' are kept in the INSERT INTO statement.

    Parameters:
    - input_path: The directory containing the .bz2 files.
    - output_dir: The directory to save the shortened files (default: 'supershorts').
    - num_rows: The number of rows to extract from the first INSERT INTO statement (default: 10).
    """
    output_path = os.path.join(input_path, output_dir)
    os.makedirs(output_path, exist_ok=True)

    # Regular expression to match INSERT INTO statements
    insert_regex = re.compile(rb'^INSERT INTO.*', re.IGNORECASE)

    # Process each .bz2 file in the input path
    for file_name in os.listdir(input_path):
        if file_name.endswith('.bz2'):
            input_file = os.path.join(input_path, file_name)
            output_file = os.path.join(output_path, file_name)

            with bz2.open(input_file, 'rb') as infile, bz2.open(output_file, 'wb') as outfile:
                insert_found = False

                for line in infile:
                    # First, check if the line contains an INSERT INTO statement
                    if insert_regex.match(line) and not insert_found:
                        # First INSERT INTO statement found
                        insert_found = True

                        # Write only the part before 'VALUES' (structure)
                        statement_start = line.split(b"VALUES")[0] + b"VALUES "
                        outfile.write(statement_start)  # Write the beginning of the INSERT INTO line (without rows)
                        continue  # Now process the next line for rows

                    # If insert statement is not found yet, write lines before it
                    if not insert_found:
                        outfile.write(line)

                    # Once the INSERT INTO statement is found, process the rows
                    elif insert_found:
                        # Process the rows of the INSERT INTO statement
                        rows = line.split(b"),(")

                        # Correct the formatting:
                        # - Remove any leading "(" from the first row
                        # - Remove trailing ");" from the last row
                        rows[0] = rows[0].lstrip(b"(")
                        rows[-1] = rows[-1].rstrip(b");")

                        # Limit to the number of rows specified
                        limited_rows = b"),(".join(rows[:num_rows])

                        # Re-add the parentheses to the output
                        limited_rows = b"(" + limited_rows + b");\n"

                        # Write the limited rows to the output file
                        outfile.write(limited_rows)

                        # **Add the closing statement ');'**
                        if not limited_rows.endswith(b");\n"):
                            outfile.write(b");\n")

                        # Stop after writing the first INSERT statement with the limited rows
                        break

            print(f"Shortened file created: {output_file}")

# Example usage
input_path = '/home/gyde/Documents/bzsets/20200920'  # Replace with your actual input path
shorten_bz2_files(input_path, num_rows=10000)
