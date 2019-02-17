

gcloud functions deploy scanit --runtime python37 --trigger-resource scanit-incoming  --trigger-event google.storage.object.finalize --timeout 540