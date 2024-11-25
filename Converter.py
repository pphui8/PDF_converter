import PyPDF2
import os
from pathlib import Path
import json
from datetime import datetime

def extract_pdf_text_in_batches(pdf_path, batch_size=500):
    """
    Extract text from PDF in batches and save progress periodically
    
    Args:
        pdf_path (str): Path to PDF file
        batch_size (int): Number of pages to process before saving
    """
    # Create output directory if it doesn't exist
    output_dir = Path("extracted_text")
    output_dir.mkdir(exist_ok=True)
    
    try:
        with open(pdf_path, 'rb') as file:
            # Create PDF reader object
            pdf_reader = PyPDF2.PdfReader(file)
            total_pages = len(pdf_reader.pages)
            
            # Initialize variables
            current_batch = []
            current_batch_num = 1
            
            print(f"Total pages in PDF: {total_pages}")
            
            # Process pages in batches
            for page_num in range(total_pages):
                try:
                    # Extract text from current page
                    page = pdf_reader.pages[page_num]
                    text = page.extract_text()
                    current_batch.append(text)
                    
                    # Save batch if we've reached batch_size or it's the last page
                    if len(current_batch) >= batch_size or page_num == total_pages - 1:
                        # Create filename with timestamp
                        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                        output_file = output_dir / f"batch_{current_batch_num}_{timestamp}.json"
                        
                        # Save batch to JSON file
                        with open(output_file, 'w', encoding='utf-8') as f:
                            json.dump({
                                'batch_number': current_batch_num,
                                'start_page': (current_batch_num - 1) * batch_size + 1,
                                'end_page': min(current_batch_num * batch_size, total_pages),
                                'text': current_batch
                            }, f, ensure_ascii=False, indent=2)
                        
                        print(f"Saved batch {current_batch_num} to {output_file}")
                        
                        # Reset batch
                        current_batch = []
                        current_batch_num += 1
                    
                    # Print progress
                    if (page_num + 1) % 500 == 0:
                        print(f"Processed {page_num + 1}/{total_pages} pages")
                        
                except Exception as e:
                    print(f"Error processing page {page_num + 1}: {str(e)}")
                    continue
            
            print("PDF processing completed!")
            
    except Exception as e:
        print(f"Error opening PDF file: {str(e)}")
        return False
    
    return True

# Usage
if __name__ == "__main__":
    pdf_path = "./harrypotter.pdf"
    success = extract_pdf_text_in_batches(pdf_path)
    
    if success:
        print("Text extraction completed successfully!")
    else:
        print("Text extraction failed!")