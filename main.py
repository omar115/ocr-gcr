import functions_framework
import fitz  # PyMuPDF
import pytesseract
from PIL import Image
import os
from google.cloud import storage
from datetime import datetime

@functions_framework.cloud_event
def hello_gcs(cloud_event):
    data = cloud_event.data
    bucket_name = data["bucket"]
    file_name = data["name"]

    print(f"Processing new file: gs://{bucket_name}/{file_name}")

    # Set up temporary directories
    tmp_dir = "/tmp"
    input_path = os.path.join(tmp_dir, file_name)
    output_dir = os.path.join(tmp_dir, "output_files")
    os.makedirs(output_dir, exist_ok=True)
    print(f"Temporary directories set up: {tmp_dir}, {output_dir}")

    text_files = []  # Initialize text_files to an empty list
    report_path = None  # Initialize report_path to None

    try:
        # Download the PDF from GCS
        print(f"Downloading PDF from GCS: {file_name}")
        download_blob(bucket_name, file_name, input_path)
        print(f"Downloaded PDF to: {input_path}")
        
        # Process the PDF
        print(f"Processing PDF: {input_path}")
        text_files = pdf_to_text(input_path, output_dir)
        print(f"Extracted text files: {text_files}")
        
        # Generate and upload report
        print(f"Generating report for: {file_name}")
        report_path = generate_report(bucket_name, file_name, output_dir)
        print(f"Generated report at: {report_path}")
        upload_blob(bucket_name, report_path, f"reports/{os.path.basename(report_path)}")
        print(f"Uploaded report to GCS: reports/{os.path.basename(report_path)}")
        
        # Upload extracted text files
        for text_file in text_files:
            dest_path = f"text_outputs/{os.path.basename(os.path.dirname(text_file))}/{os.path.basename(text_file)}"
            print(f"Uploading extracted text file: {text_file} to {dest_path}")
            upload_blob(bucket_name, text_file, dest_path)

        print("Processing completed successfully")

    except Exception as e:
        print(f"Error processing file: {str(e)}")
    finally:
        # Clean up temporary files
        print("Cleaning up temporary files...")
        for f in [input_path] + text_files + ([report_path] if report_path else []):
            try:
                os.remove(f)
                print(f"Removed temporary file: {f}")
            except Exception as cleanup_error:
                print(f"Error removing file {f}: {str(cleanup_error)}")

def pdf_to_text(pdf_path, output_dir):
    text_files = []
    print(f"Opening PDF file: {pdf_path}")
    doc = fitz.open(pdf_path)
    pdf_name = os.path.splitext(os.path.basename(pdf_path))[0]
    pdf_output_dir = os.path.join(output_dir, pdf_name)
    os.makedirs(pdf_output_dir, exist_ok=True)
    print(f"Created output directory for text files: {pdf_output_dir}")

    for page_num in range(len(doc)):
        print(f"Processing page {page_num + 1} of {pdf_name}")
        page = doc[page_num]
        pix = page.get_pixmap(dpi=300)
        img = Image.frombytes("RGB", (pix.width, pix.height), pix.samples)

        try:
            ocr_result = pytesseract.image_to_osd(img)
            rotation = int(ocr_result.split("Rotate: ")[1].split("\n")[0])
            if rotation != 0:
                img = img.rotate(-rotation, expand=True)
                print(f"Rotated page {page_num + 1} by {rotation} degrees")
        except Exception as e:
            print(f"Error during orientation detection on page {page_num + 1}: {str(e)}")
            rotation = 0

        text = pytesseract.image_to_string(img)
        output_file = os.path.join(pdf_output_dir, f"page_{page_num + 1}.txt")
        
        with open(output_file, "w", encoding="utf-8") as f:
            f.write(text)
        
        text_files.append(output_file)
        print(f"Processed page {page_num + 1}, saved to {output_file}")

    return text_files

def generate_report(bucket_name, file_name, output_dir):
    report_content = f"""Processing Report
{'='*40}
File Name: {file_name}
Bucket: {bucket_name}
Processing Time: {datetime.now().isoformat()}
"""
    report_path = os.path.join(output_dir, "processing_report.txt")
    
    with open(report_path, "w") as f:
        f.write(report_content)
    
    print(f"Report content written to {report_path}")
    return report_path

def download_blob(bucket_name, source_blob_name, destination_file_name):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(source_blob_name)
    blob.download_to_filename(destination_file_name)
    print(f"Downloaded {source_blob_name} to {destination_file_name}")

def upload_blob(bucket_name, source_file_name, destination_blob_name):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_name)
    print(f"Uploaded {source_file_name} to {destination_blob_name}")