from fastapi import FastAPI, HTTPException, Response
import psycopg2
import os
from Helpers.report import generate_report
from Helpers.constants import *
from connection import get_db_connection

app = FastAPI()

@app.get('/trigger_report')
async def trigger_report():
    try:
        # Initiates the report generation and returns the report ID
        report_id = generate_report()
        print(f'Report generation started for report ID: {report_id}')
        return {'report_id': report_id, 'message': 'Report generation initiated successfully'}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to generate report: {str(e)}")


@app.get('/get_report/{report_id}')
def get_report(report_id: str):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        #Fetches the details of the report ID given
        cursor.execute("SELECT name, status FROM reports WHERE report_id = %s", (report_id,))
        report = cursor.fetchone()

        if report:
            report_name = report[0]
            report_status = report[1]

            #If the report generation of the given report is still running corresponding message is returned
            if report_status == RUNNING_STATUS:
                return {"message": "Report is currently running"}

            #If the report generation is complete corresponding CSV file is fetched and returned
            elif report_status == COMPLETE_STATUS:
                file_path = f"Helpers/reports/{report_name}"
                if os.path.exists(file_path):
                    with open(file_path, 'rb') as file:
                        content = file.read()
                        return Response(content, media_type='application/octet-stream', headers={'Content-Disposition': f'attachment; filename="{report_name}.csv"'})
                else:
                    raise HTTPException(status_code=404, detail="Report CSV file not found")
        else:
            raise HTTPException(status_code=404, detail="Report not found")

    except psycopg2.Error as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving report: {str(e)}")
    finally:
        cursor.close()
        conn.close()