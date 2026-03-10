#!/bin/bash
set -e

S3_BUCKET="${S3_BUCKET:-aa-cl-dev-aubillatest-aubillatest-s3}"
S3_PREFIX="${S3_PREFIX:-prod}"

# Detectar jobs modificados
if [ -z "$CODEBUILD_RESOLVED_SOURCE_VERSION" ]; then
    echo "Primera ejecución - desplegando todos los jobs"
    CHANGED_FILES=$(find domains -name "*.py" -type f)
else
    git fetch origin main:main 2>/dev/null || true
    CHANGED_FILES=$(git diff --name-only HEAD origin/main | grep "^domains/.*\.py$" || echo "")
fi

if [ -z "$CHANGED_FILES" ]; then
    echo "No hay jobs modificados"
    exit 0
fi

echo "=== Jobs modificados ==="
echo "$CHANGED_FILES"
echo "======================="

# Procesar cada job
for job_file in $CHANGED_FILES; do
    [ ! -f "$job_file" ] && continue
    
    job_dir=$(dirname "$job_file")
    job_name=$(basename "$job_file" .py)
    domain=$(echo "$job_dir" | cut -d'/' -f2)
    
    echo "📦 Desplegando: $job_name ($domain)"
    
    # Subir a S3
    aws s3 cp "$job_file" "s3://${S3_BUCKET}/${S3_PREFIX}/${domain}/${job_name}.py"
    
    # Actualizar job en Glue si existe
    if aws glue get-job --job-name "$job_name" &>/dev/null; then
        aws glue update-job --job-name "$job_name" \
            --job-update Command="{Name=glueetl,ScriptLocation=s3://${S3_BUCKET}/${S3_PREFIX}/${domain}/${job_name}.py}"
        echo "✅ Job actualizado: $job_name"
    else
        echo "⚠️  Job no existe en Glue: $job_name (solo subido a S3)"
    fi
done

echo "✅ Despliegue completado"
