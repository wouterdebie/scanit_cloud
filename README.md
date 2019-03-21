# scanit_cloud

Google Cloud function that triggers on a GCS bucket `google.storage.object.finalize`
of specific JPG files, then OCRs, classifies, creates a PDF and uploads the PDF to Google Drive.

Flow:

- OCR image and convert Vision API output to HOCR
- Detect owner
- Create Searchable PDF
- Upload to Google Drive

Requirements:

- Python3
- Google Cloud Functions

## Deploy

```bash
gcloud functions deploy scanit --runtime python37 \
--trigger-resource [INCOMING_BUCKET]  \
--trigger-event google.storage.object.finalize \
--timeout 540
```

## Acknowledgements

To Konstantin Baierer for gcv2hocr.py
