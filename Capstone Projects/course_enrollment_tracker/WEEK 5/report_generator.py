
import pandas as pd

# Load final student-course-progress data
df = pd.read_csv("final_Course_Output.csv")

# Filter students with progress < 50%
low_progress = df[df["progress"] < 50]

# Save the report
low_progress.to_csv("progress_report.csv", index=False)

print("progress_report.csv generated.")
