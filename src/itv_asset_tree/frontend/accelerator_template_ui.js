/**
 * accelerator_template_ui.js - Vanilla JavaScript UI integration for Accelerator Templates.
 */

// Function to load available templates
async function loadTemplates() {
    try {
        const response = await fetch('/api/v1/template/templates/');
        const data = await response.json();

        const templateSelect = document.getElementById('templateSelect');
        templateSelect.innerHTML = '';

        if (data.available_templates && Array.isArray(data.available_templates)) {
            data.available_templates.forEach(template => {
                const option = document.createElement('option');
                option.value = template;
                option.textContent = template;
                templateSelect.appendChild(option);
            });
        } else {
            console.error('No templates found or invalid format.');
        }
    } catch (error) {
        console.error('Failed to load templates:', error);
    }
}

// Function to apply selected template
async function applyTemplate() {
    const templateType = document.getElementById('templateSelect').value;
    const parameters = JSON.parse(document.getElementById('parametersInput').value);
    const assetTree = document.getElementById('assetTreeInput').value;

    try {
        const formData = new FormData();
        formData.append('template_type', templateType);
        formData.append('parameters', JSON.stringify(parameters));
        formData.append('asset_tree_name', assetTree);

        const response = await fetch('/api/v1/template/templates/apply/', {
            method: 'POST',
            body: formData
        });

        const result = await response.json();
        alert(result.message);
    } catch (error) {
        console.error('Failed to apply template:', error);
        alert('Failed to apply template. Check console for details.');
    }
}

// Event listeners for the UI elements
document.addEventListener('DOMContentLoaded', () => {
    loadTemplates();

    document.getElementById('applyTemplateButton').addEventListener('click', applyTemplate);
});