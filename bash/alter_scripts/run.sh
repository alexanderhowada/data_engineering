argv=("$@")

path="${argv[0]}"
find_pattern="${argv[1]}"
regexp_find="${argv[2]}"
regexp_match="${argv[3]}"

files=$(find "${path}" -name "${find_pattern}")

for f in "${files[@]}"
do
    sed -i -E "s/${regexp_find}/${regexp_match}/" "${f}"
done