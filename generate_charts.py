import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from collections import Counter
import re

# Set style for professional-looking charts
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (12, 6)
plt.rcParams['font.size'] = 10

# Load the data
df = pd.read_csv('work_az_workers.csv')

print(f"Total workers in dataset: {len(df)}")
print("\nGenerating business insights charts...")

# Create charts directory if it doesn't exist
import os
os.makedirs('charts', exist_ok=True)

# ============================================
# Chart 1: Talent Pool Distribution by Experience Level
# ============================================
experience_order = ['0 il', '0 - 1 il', '1 - 3 il', '3 - 5 il', '5+ il']
experience_counts = df['experience_level'].value_counts()

# Reorder according to experience progression
experience_sorted = []
counts_sorted = []
for exp in experience_order:
    if exp in experience_counts.index:
        experience_sorted.append(exp)
        counts_sorted.append(experience_counts[exp])

plt.figure(figsize=(12, 6))
bars = plt.bar(experience_sorted, counts_sorted, color=['#FF6B6B', '#4ECDC4', '#45B7D1', '#96CEB4', '#FFEAA7'])
plt.xlabel('Experience Level', fontsize=12, fontweight='bold')
plt.ylabel('Number of Candidates', fontsize=12, fontweight='bold')
plt.title('Talent Pool Segmentation by Experience Level', fontsize=14, fontweight='bold', pad=20)
plt.xticks(rotation=0)

# Add value labels on bars
for bar in bars:
    height = bar.get_height()
    plt.text(bar.get_x() + bar.get_width()/2., height,
             f'{int(height)}',
             ha='center', va='bottom', fontweight='bold')

plt.tight_layout()
plt.savefig('charts/01_experience_distribution.png', dpi=300, bbox_inches='tight')
plt.close()

# ============================================
# Chart 2: Salary Expectations Distribution
# ============================================
salary_order = ['0₼ - 500₼', '501₼ - 1000₼', '1001₼ - 2000₼', '2001₼ - 5000₼']
df_salary = df[df['salary_range'].notna()]
salary_counts = df_salary['salary_range'].value_counts()

# Reorder
salary_sorted = []
counts_sorted = []
for sal in salary_order:
    if sal in salary_counts.index:
        salary_sorted.append(sal)
        counts_sorted.append(salary_counts[sal])

plt.figure(figsize=(12, 6))
bars = plt.bar(salary_sorted, counts_sorted, color=['#6C5CE7', '#A29BFE', '#74B9FF', '#00B894'])
plt.xlabel('Salary Range (AZN)', fontsize=12, fontweight='bold')
plt.ylabel('Number of Candidates', fontsize=12, fontweight='bold')
plt.title('Salary Expectations Across Talent Pool', fontsize=14, fontweight='bold', pad=20)
plt.xticks(rotation=15, ha='right')

for bar in bars:
    height = bar.get_height()
    plt.text(bar.get_x() + bar.get_width()/2., height,
             f'{int(height)}',
             ha='center', va='bottom', fontweight='bold')

plt.tight_layout()
plt.savefig('charts/02_salary_distribution.png', dpi=300, bbox_inches='tight')
plt.close()

# ============================================
# Chart 3: Average Salary Expectation by Experience Level
# ============================================
# Convert salary ranges to midpoints for calculation
def salary_to_midpoint(salary_str):
    if pd.isna(salary_str):
        return np.nan
    if '0₼ - 500₼' in salary_str:
        return 250
    elif '501₼ - 1000₼' in salary_str:
        return 750
    elif '1001₼ - 2000₼' in salary_str:
        return 1500
    elif '2001₼ - 5000₼' in salary_str:
        return 3500
    return np.nan

df['salary_midpoint'] = df['salary_range'].apply(salary_to_midpoint)

# Calculate average salary by experience
df_sal_exp = df[df['salary_midpoint'].notna() & df['experience_level'].notna()]
avg_salary_by_exp = df_sal_exp.groupby('experience_level')['salary_midpoint'].mean()

# Reorder
exp_labels = []
avg_salaries = []
for exp in experience_order:
    if exp in avg_salary_by_exp.index:
        exp_labels.append(exp)
        avg_salaries.append(avg_salary_by_exp[exp])

plt.figure(figsize=(12, 6))
bars = plt.bar(exp_labels, avg_salaries, color=['#FF6B6B', '#4ECDC4', '#45B7D1', '#96CEB4', '#FFEAA7'])
plt.xlabel('Experience Level', fontsize=12, fontweight='bold')
plt.ylabel('Average Expected Salary (AZN)', fontsize=12, fontweight='bold')
plt.title('Compensation Benchmarking: Salary Expectations vs Experience', fontsize=14, fontweight='bold', pad=20)
plt.xticks(rotation=0)

for bar in bars:
    height = bar.get_height()
    plt.text(bar.get_x() + bar.get_width()/2., height,
             f'{int(height)}₼',
             ha='center', va='bottom', fontweight='bold')

plt.tight_layout()
plt.savefig('charts/03_salary_vs_experience.png', dpi=300, bbox_inches='tight')
plt.close()

# ============================================
# Chart 4: Top 15 Most In-Demand Technical Skills
# ============================================
all_skills = []
for skills_str in df['technical_skills'].dropna():
    # Parse skills (format: "Skill1(LEVEL); Skill2(LEVEL); ...")
    skills_list = [s.split('(')[0].strip() for s in str(skills_str).split(';')]
    all_skills.extend(skills_list)

skill_counts = Counter(all_skills)
top_skills = skill_counts.most_common(15)

skills_names = [skill[0] for skill in top_skills]
skills_counts = [skill[1] for skill in top_skills]

plt.figure(figsize=(12, 8))
bars = plt.barh(skills_names[::-1], skills_counts[::-1], color='#E17055')
plt.xlabel('Number of Candidates', fontsize=12, fontweight='bold')
plt.ylabel('Technical Skill', fontsize=12, fontweight='bold')
plt.title('Top 15 Technical Skills in Talent Pool', fontsize=14, fontweight='bold', pad=20)

for i, bar in enumerate(bars):
    width = bar.get_width()
    plt.text(width, bar.get_y() + bar.get_height()/2.,
             f' {int(width)}',
             ha='left', va='center', fontweight='bold')

plt.tight_layout()
plt.savefig('charts/04_top_technical_skills.png', dpi=300, bbox_inches='tight')
plt.close()

# ============================================
# Chart 5: Language Proficiency Distribution
# ============================================
all_languages = []
for langs_str in df['languages'].dropna():
    # Parse languages (format: "EN(LEVEL); RU(LEVEL); ...")
    langs = re.findall(r'([A-Z]{2,})\(', str(langs_str))
    all_languages.extend(langs)

lang_counts = Counter(all_languages)
top_languages = lang_counts.most_common(10)

lang_names = [lang[0] for lang in top_languages]
lang_counts_vals = [lang[1] for lang in top_languages]

plt.figure(figsize=(12, 6))
bars = plt.bar(lang_names, lang_counts_vals, color='#6C5CE7')
plt.xlabel('Language', fontsize=12, fontweight='bold')
plt.ylabel('Number of Candidates', fontsize=12, fontweight='bold')
plt.title('Language Capabilities Across Talent Pool', fontsize=14, fontweight='bold', pad=20)
plt.xticks(rotation=0)

for bar in bars:
    height = bar.get_height()
    plt.text(bar.get_x() + bar.get_width()/2., height,
             f'{int(height)}',
             ha='center', va='bottom', fontweight='bold')

plt.tight_layout()
plt.savefig('charts/05_language_distribution.png', dpi=300, bbox_inches='tight')
plt.close()

# ============================================
# Chart 6: Technical Skills Count Distribution
# ============================================
skills_count_bins = [0, 3, 6, 10, 15, 100]
skills_count_labels = ['1-3 skills', '4-6 skills', '7-10 skills', '11-15 skills', '15+ skills']

df['skills_category'] = pd.cut(df['technical_skills_count'], bins=skills_count_bins, labels=skills_count_labels, include_lowest=True)
skills_cat_counts = df['skills_category'].value_counts().sort_index()

plt.figure(figsize=(12, 6))
bars = plt.bar(range(len(skills_cat_counts)), skills_cat_counts.values, color=['#00B894', '#00CEC9', '#0984E3', '#6C5CE7', '#FD79A8'])
plt.xlabel('Technical Skills Count', fontsize=12, fontweight='bold')
plt.ylabel('Number of Candidates', fontsize=12, fontweight='bold')
plt.title('Talent Versatility: Distribution by Technical Skills Count', fontsize=14, fontweight='bold', pad=20)
plt.xticks(range(len(skills_cat_counts)), skills_cat_counts.index, rotation=15, ha='right')

for i, bar in enumerate(bars):
    height = bar.get_height()
    plt.text(bar.get_x() + bar.get_width()/2., height,
             f'{int(height)}',
             ha='center', va='bottom', fontweight='bold')

plt.tight_layout()
plt.savefig('charts/06_skills_count_distribution.png', dpi=300, bbox_inches='tight')
plt.close()

# ============================================
# Chart 7: Recruitment Flexibility - Open to Work Status
# ============================================
open_to_work_counts = df['open_to_work_salary_by_agreement'].value_counts()
labels = ['Salary Negotiable' if x else 'Fixed Salary' for x in open_to_work_counts.index]
values = open_to_work_counts.values

plt.figure(figsize=(12, 6))
bars = plt.bar(labels, values, color=['#74B9FF', '#FD79A8'])
plt.xlabel('Salary Negotiation Preference', fontsize=12, fontweight='bold')
plt.ylabel('Number of Candidates', fontsize=12, fontweight='bold')
plt.title('Recruitment Flexibility: Salary Negotiation Preferences', fontsize=14, fontweight='bold', pad=20)

for bar in bars:
    height = bar.get_height()
    plt.text(bar.get_x() + bar.get_width()/2., height,
             f'{int(height)}',
             ha='center', va='bottom', fontweight='bold')

plt.tight_layout()
plt.savefig('charts/07_salary_flexibility.png', dpi=300, bbox_inches='tight')
plt.close()

# ============================================
# Chart 8: Education Level vs Average Technical Skills
# ============================================
edu_bins = [0, 1, 2, 3, 10]
edu_labels = ['1 degree', '2 degrees', '3 degrees', '4+ degrees']

df['education_category'] = pd.cut(df['education_count'], bins=edu_bins, labels=edu_labels, include_lowest=True)
avg_skills_by_edu = df.groupby('education_category', observed=True)['technical_skills_count'].mean()

plt.figure(figsize=(12, 6))
bars = plt.bar(range(len(avg_skills_by_edu)), avg_skills_by_edu.values, color=['#FFEAA7', '#FDCB6E', '#E17055', '#D63031'])
plt.xlabel('Education Level', fontsize=12, fontweight='bold')
plt.ylabel('Average Technical Skills Count', fontsize=12, fontweight='bold')
plt.title('Talent Quality: Education Level vs Technical Capability', fontsize=14, fontweight='bold', pad=20)
plt.xticks(range(len(avg_skills_by_edu)), avg_skills_by_edu.index, rotation=0)

for bar in bars:
    height = bar.get_height()
    plt.text(bar.get_x() + bar.get_width()/2., height,
             f'{height:.1f}',
             ha='center', va='bottom', fontweight='bold')

plt.tight_layout()
plt.savefig('charts/08_education_vs_skills.png', dpi=300, bbox_inches='tight')
plt.close()

# ============================================
# Chart 9: Top Skills for Junior vs Senior Talent
# ============================================
# Define junior and senior categories
junior_exp = ['0 il', '0 - 1 il', '1 - 3 il']
senior_exp = ['3 - 5 il', '5+ il']

df_junior = df[df['experience_level'].isin(junior_exp)]
df_senior = df[df['experience_level'].isin(senior_exp)]

# Extract top skills for each
junior_skills = []
for skills_str in df_junior['technical_skills'].dropna():
    skills_list = [s.split('(')[0].strip() for s in str(skills_str).split(';')]
    junior_skills.extend(skills_list)

senior_skills = []
for skills_str in df_senior['technical_skills'].dropna():
    skills_list = [s.split('(')[0].strip() for s in str(skills_str).split(';')]
    senior_skills.extend(skills_list)

junior_top = Counter(junior_skills).most_common(10)
senior_top = Counter(senior_skills).most_common(10)

# Create comparison chart
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))

# Junior skills
junior_names = [skill[0] for skill in junior_top]
junior_counts = [skill[1] for skill in junior_top]
bars1 = ax1.barh(junior_names[::-1], junior_counts[::-1], color='#74B9FF')
ax1.set_xlabel('Number of Candidates', fontsize=12, fontweight='bold')
ax1.set_ylabel('Technical Skill', fontsize=12, fontweight='bold')
ax1.set_title('Top 10 Skills: Junior Talent (0-3 years)', fontsize=13, fontweight='bold', pad=15)

for i, bar in enumerate(bars1):
    width = bar.get_width()
    ax1.text(width, bar.get_y() + bar.get_height()/2.,
             f' {int(width)}',
             ha='left', va='center', fontweight='bold', fontsize=9)

# Senior skills
senior_names = [skill[0] for skill in senior_top]
senior_counts = [skill[1] for skill in senior_top]
bars2 = ax2.barh(senior_names[::-1], senior_counts[::-1], color='#FD79A8')
ax2.set_xlabel('Number of Candidates', fontsize=12, fontweight='bold')
ax2.set_ylabel('Technical Skill', fontsize=12, fontweight='bold')
ax2.set_title('Top 10 Skills: Senior Talent (3+ years)', fontsize=13, fontweight='bold', pad=15)

for i, bar in enumerate(bars2):
    width = bar.get_width()
    ax2.text(width, bar.get_y() + bar.get_height()/2.,
             f' {int(width)}',
             ha='left', va='center', fontweight='bold', fontsize=9)

plt.tight_layout()
plt.savefig('charts/09_junior_vs_senior_skills.png', dpi=300, bbox_inches='tight')
plt.close()

# ============================================
# Chart 10: Average Skills and Languages by Experience
# ============================================
avg_skills_by_exp = df.groupby('experience_level')[['technical_skills_count', 'languages_count']].mean()

# Reorder
exp_labels_chart = []
skills_counts_chart = []
lang_counts_chart = []
for exp in experience_order:
    if exp in avg_skills_by_exp.index:
        exp_labels_chart.append(exp)
        skills_counts_chart.append(avg_skills_by_exp.loc[exp, 'technical_skills_count'])
        lang_counts_chart.append(avg_skills_by_exp.loc[exp, 'languages_count'])

x = np.arange(len(exp_labels_chart))
width = 0.35

fig, ax = plt.subplots(figsize=(12, 6))
bars1 = ax.bar(x - width/2, skills_counts_chart, width, label='Technical Skills', color='#00B894')
bars2 = ax.bar(x + width/2, lang_counts_chart, width, label='Languages', color='#6C5CE7')

ax.set_xlabel('Experience Level', fontsize=12, fontweight='bold')
ax.set_ylabel('Average Count', fontsize=12, fontweight='bold')
ax.set_title('Talent Capability Growth: Skills & Languages by Experience', fontsize=14, fontweight='bold', pad=20)
ax.set_xticks(x)
ax.set_xticklabels(exp_labels_chart, rotation=0)
ax.legend(fontsize=11)

# Add value labels
for bar in bars1:
    height = bar.get_height()
    ax.text(bar.get_x() + bar.get_width()/2., height,
            f'{height:.1f}',
            ha='center', va='bottom', fontweight='bold', fontsize=9)

for bar in bars2:
    height = bar.get_height()
    ax.text(bar.get_x() + bar.get_width()/2., height,
            f'{height:.1f}',
            ha='center', va='bottom', fontweight='bold', fontsize=9)

plt.tight_layout()
plt.savefig('charts/10_skills_languages_by_experience.png', dpi=300, bbox_inches='tight')
plt.close()

# ============================================
# Chart 11: Resume Availability Analysis
# ============================================
df['has_resume'] = df['resume_url'].notna()
resume_counts = df['has_resume'].value_counts()
labels = ['Resume Available' if x else 'No Resume' for x in resume_counts.index]
values = resume_counts.values

plt.figure(figsize=(12, 6))
bars = plt.bar(labels, values, color=['#00B894', '#E17055'])
plt.xlabel('Resume Status', fontsize=12, fontweight='bold')
plt.ylabel('Number of Candidates', fontsize=12, fontweight='bold')
plt.title('Recruitment Readiness: Resume Availability', fontsize=14, fontweight='bold', pad=20)

for bar in bars:
    height = bar.get_height()
    plt.text(bar.get_x() + bar.get_width()/2., height,
             f'{int(height)} ({int(height)/len(df)*100:.1f}%)',
             ha='center', va='bottom', fontweight='bold')

plt.tight_layout()
plt.savefig('charts/11_resume_availability.png', dpi=300, bbox_inches='tight')
plt.close()

# ============================================
# Chart 12: Multilingual Talent Analysis
# ============================================
lang_bins = [0, 2, 3, 4, 10]
lang_labels = ['1-2 languages', '3 languages', '4 languages', '5+ languages']

df['language_category'] = pd.cut(df['languages_count'], bins=lang_bins, labels=lang_labels, include_lowest=True)
lang_cat_counts = df['language_category'].value_counts().sort_index()

plt.figure(figsize=(12, 6))
bars = plt.bar(range(len(lang_cat_counts)), lang_cat_counts.values, color=['#74B9FF', '#A29BFE', '#FD79A8', '#FFEAA7'])
plt.xlabel('Language Proficiency Count', fontsize=12, fontweight='bold')
plt.ylabel('Number of Candidates', fontsize=12, fontweight='bold')
plt.title('Global Market Readiness: Multilingual Talent Distribution', fontsize=14, fontweight='bold', pad=20)
plt.xticks(range(len(lang_cat_counts)), lang_cat_counts.index, rotation=15, ha='right')

for bar in bars:
    height = bar.get_height()
    plt.text(bar.get_x() + bar.get_width()/2., height,
             f'{int(height)}',
             ha='center', va='bottom', fontweight='bold')

plt.tight_layout()
plt.savefig('charts/12_multilingual_distribution.png', dpi=300, bbox_inches='tight')
plt.close()

print("\n✓ All 12 business insight charts generated successfully in the 'charts/' directory!")
print("\nCharts created:")
print("  1. Talent Pool Segmentation by Experience Level")
print("  2. Salary Expectations Across Talent Pool")
print("  3. Compensation Benchmarking: Salary vs Experience")
print("  4. Top 15 Technical Skills in Talent Pool")
print("  5. Language Capabilities Across Talent Pool")
print("  6. Talent Versatility: Distribution by Technical Skills Count")
print("  7. Recruitment Flexibility: Salary Negotiation Preferences")
print("  8. Talent Quality: Education Level vs Technical Capability")
print("  9. Top Skills for Junior vs Senior Talent")
print(" 10. Talent Capability Growth: Skills & Languages by Experience")
print(" 11. Recruitment Readiness: Resume Availability")
print(" 12. Global Market Readiness: Multilingual Talent Distribution")
