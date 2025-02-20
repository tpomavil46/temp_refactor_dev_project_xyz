// src/itv_asset_tree/frontend/accelerator_template_ui.js

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

// Function to load available templates into the dropdown
async function loadTemplates() {
    try {
        console.log("🔄 Fetching available templates...");
        const response = await fetch('/api/v1/template/templates/');
        const data = await response.json();

        const templateSelect = document.getElementById('templateSelect');

        if (!templateSelect) {
            console.error("❌ Template dropdown not found in HTML.");
            return;
        }

        // Clear existing options
        templateSelect.innerHTML = '';

        if (data.available_templates && Array.isArray(data.available_templates)) {
            data.available_templates.forEach(template => {
                const option = document.createElement('option');
                option.value = template;
                option.textContent = template;
                templateSelect.appendChild(option);
            });

            // ✅ Set default selected template
            templateSelect.value = data.available_templates[0];

            console.log("✅ Templates loaded successfully:", data.available_templates);
        } else {
            console.error('⚠️ No templates found or invalid response format.');
        }
    } catch (error) {
        console.error('❌ Failed to load templates:', error);
    }
}

// Function to map the user-selected type to a valid Seeq type
function getSeeqType(selectedType, templateType) {
    if (selectedType === "Calculations") {
        console.log("🔄 Mapping 'Calculations' to correct type...");
        
        if (templateType === "HVAC_With_Calcs") {
            return "CalculatedSignal";  // Default, as HVAC_With_Calcs applies to signals
        }
        
        return "CalculatedSignal";  // Fallback if not recognized
    }
    return selectedType; // Otherwise, return the selected type directly
}

// Function to apply the selected template
async function applyTemplate() {
    const templateSelect = document.getElementById('templateSelect');
    const typeInput = document.getElementById('typeInput');
    const searchQuery = document.getElementById('searchQueryInput').value;
    const type = getSeeqType(typeInput.options[typeInput.selectedIndex].value, templateSelect.value);
    const datasourceName = document.getElementById('datasourceInput').value;
    const assetTree = document.getElementById('assetTreeInput').value;
    const statusElement = document.getElementById('templateStatus');
    const buildPath = document.getElementById('buildPathInput').value;

    // Auto-switch template when "Calculations" is selected
    if (typeInput.value === "Calculations") {
        console.log("🔄 Switching to HVAC_With_Calcs...");
        templateSelect.value = "HVAC_With_Calcs";
    }

    const templateType = templateSelect.value;

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
            type: type, // ✅ Now dynamically mapped
            datasource_name: datasourceName,
            build_asset_regex: "(Area .)_.*",
            build_path: buildPath || "My HVAC Units >> Facility #1",
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

// Ensure event listener is attached when DOM is loaded
document.addEventListener('DOMContentLoaded', () => {
    console.log("✅ Document Loaded.");

    const templateSelect = document.getElementById('templateSelect');
    const applyTemplateButton = document.getElementById('applyTemplateButton');
    const typeInput = document.getElementById("typeInput");

    if (!templateSelect || !applyTemplateButton || !typeInput) {
        console.error("❌ Missing required elements in HTML.");
        return;
    }

    // ✅ Load templates after ensuring the dropdown exists
    loadTemplates();

    // ✅ Detect changes to typeInput and switch template dynamically
    typeInput.addEventListener("change", function () {
        const selectedType = this.value;
        const baseTemplate = templateSelect.value.replace("_With_Calcs", ""); // Ensure we have base template
        
        if (selectedType === "Calculations") {
            const calcTemplate = `${baseTemplate}_With_Calcs`;
            console.log(`🔄 Switching to ${calcTemplate}...`);
            templateSelect.value = calcTemplate;
        } else {
            console.log(`🔄 Switching back to base template: ${baseTemplate}`);
            templateSelect.value = baseTemplate;
        }
    });

    // ✅ Refresh UI when switching templates
    templateSelect.addEventListener("change", () => {
        console.log(`🔄 Switched to template: ${templateSelect.value}`);
    });

    applyTemplateButton.addEventListener('click', applyTemplate);
});