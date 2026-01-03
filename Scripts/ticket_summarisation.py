import pandas as pd
import os
# Load the CSV file into a Pandas DataFrame
file_path = 'ticket_dump.csv'  # Replace with your file path
output_file='ticket_analysis_output.csv'

if os.path.exists(output_file):
    os.remove(output_file)
df = pd.read_csv(file_path)

# Basic data preprocessing
df.fillna('', inplace=True)  # Fill missing values with empty strings

# Calculate ticket counts for each issue, sub-issue category, and issue reason
issue_counts = df.groupby(['Issue Category', 'Sub Issue Category', 'Issue reason']).size().reset_index(name='Count')

# Calculate the total number of tickets
total_tickets = len(df)

# Compute percentage for each issue/sub-issue category/reason
issue_counts['Percentage'] = (issue_counts['Count'] / total_tickets) * 100

# Sort by count in descending order and select top 15
top_issues = issue_counts.sort_values(by='Count', ascending=False).head(45)

# Prepare reasons (from Remarks column) for the top issues
def top_5_remarks(remarks):
    return ' | '.join(remarks.value_counts().head(5).index)

reasons = df.groupby(['Issue Category', 'Sub Issue Category', 'Issue reason'])['Remarks'].apply(top_5_remarks).reset_index()

# Merge reasons with the top issues
top_issues_with_reasons = pd.merge(top_issues, reasons, on=['Issue Category', 'Sub Issue Category', 'Issue reason'])

# Compute cumulative percentage
top_issues_with_reasons['Cumulative Percentage'] = top_issues_with_reasons['Percentage'].cumsum()

# Print the results to the console
print(top_issues_with_reasons[['Issue Category', 'Sub Issue Category', 'Issue reason', 'Count', 'Percentage', 'Cumulative Percentage', 'Remarks']])

# Write the results to a CSV file
output_file_path = 'ticket_analysis_output.csv'  # Replace with your desired output path
top_issues_with_reasons.to_csv(output_file_path, index=False)
