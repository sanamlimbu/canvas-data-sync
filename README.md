# canvas-data-sync

Currently, the script syncs tables under **_canvas_** schema.

## Environment variables

DAP_API_URL\
DAP_CLIENT_ID\
DAP_CLIENT_SECRET\
Create the above envs from [Instructure Identity](https://identity.instructure.com/).\
DAP_CONNECTION_STRING is your database connection string.

## Dependencies

Python 3.4 or later\
instructure-dap-client

Install required dependencies using\
`pip install -r requirements.txt`

## Run

`python canvas.py local`
`python canvas.py supabase`
