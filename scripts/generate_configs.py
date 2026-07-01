import os
import re

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ENV_PATH = os.path.join(BASE_DIR, ".env")
TEMPLATES = {
    os.path.join(BASE_DIR, "prometheus", "alertmanager.yml.tmpl"):
        os.path.join(BASE_DIR, "prometheus", "alertmanager.yml"),
}


def load_env():
    env = {}
    if not os.path.exists(ENV_PATH):
        print(f"[WARN] .env not found at {ENV_PATH}")
        return env

    with open(ENV_PATH, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, val = line.partition("=")
            env[key.strip()] = val.strip().strip('"').strip("'")
    return env


def generate(template_path, output_path, env):
    if not os.path.exists(template_path):
        print(f"[WARN] Template not found: {template_path}")
        return False

    with open(template_path, encoding="utf-8") as f:
        content = f.read()

    def replace_var(match):
        key = match.group(1)
        return env.get(key, match.group(0))

    content = re.sub(r"__(\w+)__", replace_var, content)

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(content)

    print(f"[OK] Generated {output_path}")
    return True


def main():
    env = load_env()
    print(f"[INFO] Loaded {len(env)} env vars from .env")
    for template, output in TEMPLATES.items():
        generate(template, output_path=output, env=env)


if __name__ == "__main__":
    main()
