# Intel Media Pulse

## הפעלה מהירה

```bash
pip install -r requirements.txt
set TAVILY_API_KEY=your-key-here
python app.py
```

ואז לפתוח בדפדפן:

[http://localhost:8000](http://localhost:8000)

## הערות

- אם אין `TAVILY_API_KEY`, המערכת עדיין תעבוד עם Google News RSS וחילוץ מטא־דאטה ישיר.
- בהרצה ראשונה האפליקציה מפעילה אוטומטית גילוי מקורות ואיסוף כתבות ברקע.
- קובץ `sources.json` הוא רג'יסטרי פתוח לעריכה ידנית.
