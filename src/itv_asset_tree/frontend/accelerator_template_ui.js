/**
 * accelerator_template_ui.js - Vanilla JavaScript UI integration for Accelerator Templates.
 */

// ✅ Define this function FIRST
async function loadTemplateParameters() {
    const templateType = document.getElementById('templateSelect').value;
    if (!templateType) {
        console.error("❌ No template selected.");
        return;
    }

    console.log(`🔍 Fetching parameters for template: ${templateType}`);

    try {
        const response = await fetch(`/api/v1/template/templates/${templateType}/parameters`);
        const data = await response.json();
        console.log("✅ Parameters received:", data);

        if (data.required_parameters) {
            document.getElementById('parametersInput').value = JSON.stringify(data.required_parameters, null, 2);
        } else {
            document.getElementById('parametersInput').value = '{}';
        }
    } catch (error) {
        console.error("❌ Failed to load template parameters:", error);
        document.getElementById('parametersInput').value = '{}';
    }
}

// ✅ Now define `loadTemplates()`
async function loadTemplates() {
    try {
        const response = await fetch('/api/v1/template/templates/');
        const data = await response.json();

        const templateSelect = document.getElementById('templateSelect');
        templateSelect.innerHTML = '';

        if (data.available_templates && Array.isArray(data.available_templates)) {
            data.available_templates.forEach(template => {
                const option = document.createElement('option');
                option.value = template.name;  // Ensure we use the correct key
                option.textContent = template.name;
                templateSelect.appendChild(option);
            });

            // ✅ Auto-load parameters for the first template in the list
            if (data.available_templates.length > 0) {
                templateSelect.value = data.available_templates[0].name;
                loadTemplateParameters();  // Now it is defined and will work
            }
        } else {
            console.error('❌ No templates found or invalid format.');
        }
    } catch (error) {
        console.error('❌ Failed to load templates:', error);
    }
}


// Function to apply selected template
async function applyTemplate() {
    const templateType = document.getElementById('templateSelect').value;
    const assetTree = document.getElementById('assetTreeInput').value;
    const parametersInput = document.getElementById('parametersInput').value.trim();
    const statusElement = document.getElementById('templateStatus');

    if (!templateType) {
        alert("⚠️ Please select a template.");
        return;
    }

    if (!assetTree) {
        alert("⚠️ Please enter an Asset Tree name.");
        return;
    }

    let parameters;
    try {
        parameters = parametersInput ? JSON.parse(parametersInput) : {};
    } catch (error) {
        alert("⚠️ Invalid JSON format in parameters.");
        return;
    }

    statusElement.innerText = "⏳ Applying template...";

    try {
        const formData = new FormData();
        formData.append('template_name', templateType);
        formData.append('parameters', JSON.stringify(parameters));
        formData.append('asset_tree_name', assetTree);

        const response = await fetch('/api/v1/template/build', {
            method: 'POST',
            body: formData
        });

        const result = await response.json();
        
        if (response.ok) {
            statusElement.innerText = `✅ ${result.message}`;
            console.log("✅ Template applied successfully:", result);
        } else {
            statusElement.innerText = `❌ Error: ${result.detail}`;
            console.error("❌ Failed to apply template:", result);
        }
    } catch (error) {
        console.error('❌ Failed to apply template:', error);
        statusElement.innerText = "❌ Failed to apply template. Check console for details.";
    }
}

// Event listeners for the UI elements
document.addEventListener('DOMContentLoaded', () => {
    loadTemplates();
    loadTemplateParameters();
    document.getElementById('applyTemplateButton').addEventListener('click', applyTemplate);
});