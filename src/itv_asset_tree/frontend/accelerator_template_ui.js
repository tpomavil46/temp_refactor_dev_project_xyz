/**
 * accelerator_template_ui.js - Vanilla JavaScript UI integration for Accelerator Templates.
 */

/// Function to load template parameters dynamically
async function loadTemplateParameters() {
    const templateType = document.getElementById('templateSelect').value;
    console.log(`🔍 Fetching parameters for template: ${templateType}`);

    try {
        const response = await fetch(`/api/v1/template/templates/${templateType}/parameters`);
        const data = await response.json();
        console.log("✅ Parameters received:", data);

        // ✅ CLEAR the search fields (user should enter manually)
        document.getElementById("searchQueryInput").value = "";
        document.getElementById("typeInput").value = "";
        document.getElementById("datasourceInput").value = "";

    } catch (error) {
        console.error("❌ Failed to load template parameters:", error);
    }
}

// Now define `loadTemplates()`
async function loadTemplates() {
    try {
        console.log("🔄 Fetching available templates...");
        const response = await fetch('/api/v1/template/templates/');
        const data = await response.json();

        const templateSelect = document.getElementById('templateSelect');

        // ✅ Ensure the dropdown is not null
        if (!templateSelect) {
            console.error("❌ Template dropdown element not found!");
            return;
        }

        // ✅ Clear existing options
        templateSelect.innerHTML = '<option value="">-- Select a Template --</option>';

        if (data.available_templates && Array.isArray(data.available_templates)) {
            data.available_templates.forEach(template => {
                const option = document.createElement('option');
                option.value = template.name;  // Ensure correct key is used
                option.textContent = template.name;
                templateSelect.appendChild(option);
            });

            console.log("✅ Templates loaded:", data.available_templates);
        } else {
            console.error("⚠️ No templates found or invalid response format.");
        }
    } catch (error) {
        console.error("❌ Failed to load templates:", error);
    }
}

// Function to apply the selected template
async function applyTemplate() {
    const templateType = document.getElementById('templateSelect').value;
    const searchQuery = document.getElementById('searchQueryInput').value;
    const type = document.getElementById('typeInput').value;
    const datasourceName = document.getElementById('datasourceInput').value;
    const assetTree = document.getElementById('assetTreeInput').value;
    const statusElement = document.getElementById('templateStatus');

    if (!templateType) {
        alert("⚠️ Please select a template.");
        return;
    }
    if (!searchQuery || !type || !datasourceName) {
        alert("⚠️ Please fill out the Search Query, Type, and Datasource Name fields.");
        return;
    }

    statusElement.innerText = "⏳ Applying template...";

    try {
        const payload = {
            template_name: templateType,
            search_query: searchQuery,
            type: type,
            datasource_name: datasourceName,
            build_asset_regex: "(Area .)_.*",  // Default regex (user can modify)
            build_path: "My HVAC Units >> Facility #1", // Default path (user can modify)
        };

        console.log("🔍 Payload Sent to FastAPI:", JSON.stringify(payload, null, 2));

        const response = await fetch('/api/v1/template/build', {
            method: 'POST',
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(payload)
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

document.addEventListener('DOMContentLoaded', () => {
    console.log("✅ Document Loaded.");

    const templateSelect = document.getElementById('templateSelect');
    const applyTemplateButton = document.getElementById('applyTemplateButton');

    if (!templateSelect || !applyTemplateButton) {
        console.error("❌ Missing required elements in HTML.");
        return;
    }

    // ✅ Load templates after ensuring the dropdown exists
    loadTemplates();

    // ✅ Refresh UI when switching templates
    templateSelect.addEventListener("change", () => {
        console.log(`🔄 Switched to template: ${templateSelect.value}`);
    });

    applyTemplateButton.addEventListener('click', applyTemplate);
});