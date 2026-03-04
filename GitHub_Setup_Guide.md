# GitHub Repository & Pages Setup Guide
## For: Avinash Somisetty

---

## STEP 1 — Create GitHub Account (skip if exists)

1. Go to https://github.com/join
2. Sign up as **avinash-somisetty** (username)
3. Verify email

---

## STEP 2 — Create the Repository

1. Go to https://github.com/new
2. Fill in:
   - **Repository name**: `avinash-somisetty.github.io`
     (IMPORTANT: must match the username exactly — this enables GitHub Pages at the root URL)
   - **Description**: "Product Manager portfolio & automation projects"
   - **Visibility**: Public
   - **DO NOT** initialize with README (we'll push our own)
3. Click **Create repository**

---

## STEP 3 — Install Git (if not already installed)

**Mac:**
```bash
brew install git
```

**Windows:**
Download from https://git-scm.com/download/win

**Verify:**
```bash
git --version
```

---

## STEP 4 — Configure Git

```bash
git config --global user.name "Avinash Somisetty"
git config --global user.email "aviforavinash@gmail.com"
```

---

## STEP 5 — Extract & Push the Repository

### 5a. Extract the downloaded zip

Unzip `avinash-portfolio-repo.zip` to a folder. You'll see:

```
avinash-portfolio/
├── .gitignore
├── README.md
├── docs/
│   └── index.html          ← Portfolio website
└── projects/
    └── bess-report-automation/
        ├── README.md
        ├── requirements.txt
        ├── bess_report_engine.py
        ├── bess_report_pdf.py
        ├── bess_report_generator.py
        └── sample_output.html
```

### 5b. Initialize and push

Open Terminal / Command Prompt, navigate to the extracted folder:

```bash
cd path/to/avinash-portfolio

# Initialize git repo
git init

# Add all files
git add .

# First commit
git commit -m "Initial commit: portfolio site + BESS report automation"

# Set main branch
git branch -M main

# Connect to GitHub (replace with your actual repo URL)
git remote add origin https://github.com/avinash-somisetty/avinash-somisetty.github.io.git

# Push
git push -u origin main
```

If prompted for credentials, use a **Personal Access Token** (not password):
1. Go to GitHub → Settings → Developer Settings → Personal Access Tokens → Tokens (classic)
2. Generate new token with `repo` scope
3. Use this token as the password when prompted

---

## STEP 6 — Enable GitHub Pages

1. Go to your repo: `https://github.com/avinash-somisetty/avinash-somisetty.github.io`
2. Click **Settings** (tab)
3. In the left sidebar, click **Pages**
4. Under **Source**, select:
   - **Branch**: `main`
   - **Folder**: `/docs`
5. Click **Save**
6. Wait 1-2 minutes for deployment

---

## STEP 7 — Verify

Your portfolio is now live at:
**https://avinash-somisetty.github.io**

The BESS project is at:
**https://github.com/avinash-somisetty/avinash-somisetty.github.io/tree/main/projects/bess-report-automation**

---

## FUTURE — Adding More Projects

To add a new project:

```bash
# Create a new folder under projects/
mkdir -p projects/new-project-name

# Add your files there
# Then:
git add .
git commit -m "Add: new-project-name"
git push
```

To update the portfolio page, edit `docs/index.html` and push.

---

## TROUBLESHOOTING

**GitHub Pages not loading?**
- Ensure repo name is exactly `avinash-somisetty.github.io`
- Check Settings → Pages → Source is set to `main` branch, `/docs` folder
- Wait 2-3 minutes after enabling — first deployment takes time

**Push rejected?**
- Make sure the repo was created WITHOUT initializing README
- If it has files already: `git pull origin main --allow-unrelated-histories` then push

**Want to use a custom domain later?**
- Buy a domain (e.g., avinashsomisetty.com)
- Add a CNAME file in `docs/` with just the domain name
- Configure DNS: CNAME record pointing to `avinash-somisetty.github.io`
