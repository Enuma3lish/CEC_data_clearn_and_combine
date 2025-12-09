def normalize_code(c):
    return c.strip().replace("'", "").replace('"', "").lstrip('0') or '0'

prv_from_file = "'10"
normalized = normalize_code(prv_from_file).zfill(5)
target = "10015"

print(f"From file: {prv_from_file}")
print(f"Normalized: {normalized}")
print(f"Target: {target}")
print(f"Match: {normalized == target}")
