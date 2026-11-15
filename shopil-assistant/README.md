# מטלה 1: עוזר עסקי ל-ShopIL (Batch + Streaming + יוצר ומעריך)

<div dir="rtl">

המערכת מסבירה את מצב החנות מתוך נתוני עבר (dlt → DuckDB → dbt) ומתוך זרם הזמנות וביקורות חי (מנוע `StreamEngine` עם SQLite).
צ'אט Gradio עונה על שאלות בעברית: **יוצר** (Actor) מנסח תשובה מתוך חבילת ראיות קבועה, ו**מעריך** (Critic) בודק אותה. לולאת תיקון של עד 3 ניסיונות.

## מבנה התיקייה

```
פרויקט/
  etl-course-demo/        חלק א. שוכפל ממאגר הקורס; הושלמו בו 4 קבצי התרגיל ונוספה בדיקת dbt אחת (ראו להלן)
  streaming-course-demo/  חלק ב. מנוע הקורס ללא שינוי; ניסויי חלק ב כתובים ב-shopil-assistant/experiments
  shopil-assistant/       הקוד שפותח בעבודה זו: שכבת שליפה, יוצר/מעריך, צ'אט, ניסויים, ראיות
    shopil/               retrieval.py · actor_critic.py · llm.py · live_ingest.py · app.py · config.py
    prompts/              actor_he.txt · critic_he.txt  (הנחיות שני המודלים)
    experiments/          partA_verify.py · partB_streaming.py · partCD_mock.py · partCD_real.py · verify_real_answers.py · take_screenshots.py
    evidence/             תוצרי הרצה: פלטי dbt, CSV של חלונות/איחורים, יומנים, צילומי מסך
    logs/                 audit.jsonl (טיוטות, החלטות, משוב) · llm_calls.jsonl · evidence/<snapshot>.json
    run_app.py  .env.example  requirements.lock.txt
```

## מה נלקח מהקורס ומה פותח בעבודה

| רכיב | מקור |
|---|---|
| `etl-course-demo`: `02_dlt_pipeline.py`, מודלי dbt של orders/customers/products, `generate_data.py`, `check_exercise.py` | קורס, ללא שינוי |
| `streaming-course-demo`: `StreamEngine`, `run_live`, `run_microbatch`, `events.py` (הנתונים והמנוע) | קורס, ללא שינוי |
| **הושלם בעבודה (תרגיל הביקורות)**: `pipelines/04_reviews_pipeline.py`, `models/staging/stg_reviews.sql`, `models/marts/mart_product_reviews.sql`, `models/marts/_reviews.yml` | נכתב בעבודה, מתוך קובצי ההתחלה |
| **נוסף בעבודה**: `dbt_project/tests/assert_mart_reviews_no_double_count.sql` (בדיקה נגד הכפלת הכנסה בחיבור הזמנות-ביקורות; מוכחת ככזו שנכשלת על מארט שגוי, ראו `evidence/partA/10_*`) | נכתב בעבודה |
| גרסאות מאגרי הקורס שעליהן העבודה בנויה | `etl-course-demo` 6788526695bb… · `streaming-course-demo` c27b4fc1b343… (ללא שינוי בקובצי הקורס; מומלץ `git clone` ואז העתקת הקבצים שלעיל) |
| **נכתב בעבודה**: כל `shopil-assistant/` (שכבת שליפה, חבילת ראיות וצילום מצב, יוצר ומעריך, Pydantic `Evaluation`, ממשק Gradio, הקליטה ברקע, כל הניסויים והמאמתים) | פותח בעבודה |
| חלק הפתרון של המרצה (`exercises/solution/`) | **לא נקרא ולא בשימוש** |

## התקנה (Windows, PowerShell, uv)

הסביבות נפרדות ומבודדות; Python 3.13.12 מנוהל על ידי uv ואינו משנה את Python של המערכת.

```powershell
# 1) ETL (חלק א)
cd etl-course-demo
uv venv --python 3.13.12
uv pip install --python .venv\Scripts\python.exe -r requirements-notebook.txt

# 2) Streaming (חלק ב)
cd ..\streaming-course-demo
uv sync --locked

# 3) העוזר (חלק ג)
cd ..\shopil-assistant
uv venv --python 3.13.12
uv pip install --python .venv\Scripts\python.exe gradio openai pydantic duckdb python-dotenv playwright -e ../streaming-course-demo
copy .env.example .env      # ואז למלא LLM_API_KEY (ללא מפתחות בקוד)
```

משתני סביבה (`.env.example`): `LLM_API_KEY`, `LLM_BASE_URL` (ברירת מחדל OpenRouter, תואם OpenAI), `LLM_MODEL`, `LLM_MODEL_FAST`. מודלים חינמיים (`:free`) מספיקים.

## הרצה

**חלק א, טעינה ומדדים** (מתוך `etl-course-demo`):
```powershell
$py = ".venv\Scripts\python.exe"
& $py pipelines/02_dlt_pipeline.py --offline        # לקוחות, מוצרים, הזמנות (merge)
& $py pipelines/04_reviews_pipeline.py              # ביקורות (replace)
cd dbt_project; ..\.venv\Scripts\dbt.exe build --profiles-dir .; cd ..
& $py exercises/check_exercise.py 1 2 3             # בודק התרגיל: 14/14
& $py ..\shopil-assistant\experiments\partA_verify.py warehouse.duckdb data\raw\reviews.json
```

**חלק ב, זרם** (שני המצבים והניסויים):
```powershell
cd shopil-assistant
..\streaming-course-demo\.venv\Scripts\python.exe experiments\partB_streaming.py evidence\partB
# הדגמות CLI של הקורס: uv run python -m streaming_lab live --seconds 4 --queue-size 8  (בתוך streaming-course-demo)
```

**חלק ג, צ'אט** (הקליטה ממשיכה לפעול ברקע בזמן השיחה; חלק א חייב להיות טעון לפני כן):
```powershell
.venv\Scripts\python.exe run_app.py --fresh-stream      # http://127.0.0.1:7860
```

**בדיקות והוכחות (חלקים ג-ד)**:
```powershell
.venv\Scripts\python.exe experiments\partCD_mock.py evidence\partCD            # בדיקות מדומות (MOCK), ללא מפתח
.venv\Scripts\python.exe experiments\partCD_real.py evidence\partCD_real       # מודלים אמיתיים (דורש מפתח; כ-9 קריאות)
.venv\Scripts\python.exe experiments\verify_real_answers.py evidence\partCD_real   # אימות ידני של התשובות מול SQL ישיר
.venv\Scripts\python.exe experiments\take_screenshots.py evidence\screenshots      # צילומי מסך אמיתיים של האפליקציה
```

## החלטות מרכזיות

- **Batch:** הזמנות ב-`merge` לפי `order_id` ו-`updated_at` (קובץ אינקרמנטלי; ריצה חוזרת כותבת 0 שורות חדשות). ביקורות ב-`replace` (צילום מצב מלא; כל ריצה מחליפה את הטבלה). הניקוי רק ב-dbt.
- **zone זמן:** `created_at` נטען כ-timestamptz; ב-`stg_reviews` מומר עם `timezone('UTC', …)` כדי שהתוצאה לא תלויה באזור הזמן של המחשב (באג שנמצא בבדיקה עצמאית).
- **צילום מצב לשאלה:** `retrieval.build_evidence` קורא את מצב הזרם בטרנזקציית קריאה אחת (WAL), יחד עם ה-checkpoint, ומקפיא JSON אחד. היוצר, המעריך וכל ניסיון תיקון מקבלים את אותה מחרוזת (sha256 נרשם ביומן).
- **הפרדת מקורות:** Batch = הזמנות ששולמו בשקלים; זרם = אירועי הזמנה באגורות ללא מצב תשלום. החבילה כוללת `source_comparison` (התאמת מזהי מוצר, אין חפיפת `order_id`, טווחי זמן שונים) ו-`combined_total_allowed=false`.
- **גבול טרנזקציה:** כל אירוע נשמר בטרנזקציית SQLite אחת (checkpoint, מונים, חלונות, side outputs). שמירה מקומית אינה מוכיחה exactly-once מול מערכת חיצונית.
- **כל הטיוטות שנדחו** נשמרות ב-`logs/audit.jsonl` בלבד, לא בשיחה.

</div>
