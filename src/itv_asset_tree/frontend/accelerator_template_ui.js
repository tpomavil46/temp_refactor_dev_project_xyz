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

async function applyTemplate() {
    console.log("🚀 Applying template...");

    const templateSelect = document.getElementById('templateSelect');
    const typeInput = document.getElementById('typeInput');

    if (!templateSelect) {
        console.error("❌ templateSelect not found in the DOM!");
        return;
    }
    if (!typeInput) {
        console.error("❌ typeInput not found in the DOM!");
        return;
    }

    // ✅ Ensure all necessary fields exist
    const searchQuery = document.getElementById('searchQueryInput')?.value.trim() || "";
    const datasourceName = document.getElementById('datasourceInput')?.value.trim() || "";
    const workbookName = document.getElementById('workbookNameInput')?.value.trim() || "";
    const buildPath = document.getElementById('buildPathInput')?.value.trim() || "";
    const calculationsTemplate = document.getElementById('calculationsTemplateInput')?.value.trim() || "";

    // ✅ Base template is selected from the dropdown
    const baseTemplate = templateSelect.value; 

    const statusElement = document.getElementById('templateStatus');
    if (!statusElement) {
        console.error("❌ statusElement not found in the DOM!");
        return;
    }

    const type = getSeeqType(typeInput.options[typeInput.selectedIndex]?.value || "", templateSelect.value);

    if (!templateSelect.value) {
        alert("⚠️ Please select a template.");
        return;
    }

    let payload = {
        template_name: templateSelect.value,
        type: type,
        build_asset_regex: "(Area .)_.*",
        build_path: buildPath || "My HVAC Units >> Facility #1",
        workbook_name: workbookName,
        search_query: searchQuery,
        base_template: baseTemplate,
        calculations_template: calculationsTemplate,
        datasource_name: datasourceName  // ✅ Ensure this is included!
    };

    let apiEndpoint = "/api/v1/template/build";  // Default for Stored Signals

    if (type.includes("Calculated")) {
        if (!workbookName || !baseTemplate || !calculationsTemplate) {
            alert("⚠️ Please provide required fields for Calculated Signals (Workbook Name, Base Template, and Calculations Template).");
            return;
        }
        payload.base_template = baseTemplate;  // ✅ Added missing field
        payload.calculations_template = calculationsTemplate;
        apiEndpoint = "/api/v1/template/build_calculated";  // ✅ New endpoint
    } else {
        if (!datasourceName) {
            alert("⚠️ Please provide a Datasource Name for Stored Signals.");
            return;
        }
        payload.datasource_name = datasourceName;
    }

    statusElement.innerText = "⏳ Applying template...";

    try {
        console.log("🔍 Payload Sent to FastAPI:", JSON.stringify(payload, null, 2));

        const response = await fetch(apiEndpoint, {
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
document.addEventListener("DOMContentLoaded", () => {
    console.log("✅ Document Loaded.");

    const templateSelect = document.getElementById("templateSelect");
    const applyTemplateButton = document.getElementById("applyTemplateButton");
    const typeInput = document.getElementById("typeInput");

    // Get field containers for visibility toggling
    const datasourceContainer = document.getElementById("datasourceContainer");
    const searchQueryContainer = document.getElementById("searchQueryContainer");
    const workbookNameContainer = document.getElementById("workbookNameContainer");
    const buildPathContainer = document.getElementById("buildPathContainer");
    const templateContainer = document.getElementById("templateContainer"); // Previously Select Template
    const calculationsTemplateContainer = document.getElementById("calculationsTemplateContainer"); // New Calculations Template

    if (!templateSelect || !applyTemplateButton || !typeInput ||
        !datasourceContainer || !searchQueryContainer || !workbookNameContainer ||
        !buildPathContainer || !templateContainer || !calculationsTemplateContainer) {
        console.error("❌ Missing required elements in HTML.");
        return;
    }

    // ✅ Load templates after ensuring the dropdown exists
    loadTemplates();

    // Function to update form fields dynamically based on Type selection
    function updateFormFields() {
        console.log("🛠 Running updateFormFields...");
    
        const typeInput = document.getElementById("typeInput");
        const selectedType = typeInput ? typeInput.value.trim() : null;
        if (!selectedType) {
            console.error("❌ Type input is missing!");
            return;
        }
    
        const datasourceContainer = document.getElementById("datasourceContainer");
        const searchQueryContainer = document.getElementById("searchQueryContainer");
        const workbookNameContainer = document.getElementById("workbookNameContainer");
        const buildPathContainer = document.getElementById("buildPathContainer");
        const templateContainer = document.getElementById("templateContainer");
        const calculationsTemplateContainer = document.getElementById("calculationsTemplateContainer");
    
        // Debug what exists
        console.log("🔍 Checking container elements...");
        console.log("datasourceContainer:", datasourceContainer);
        console.log("searchQueryContainer:", searchQueryContainer);
        console.log("workbookNameContainer:", workbookNameContainer);
        console.log("buildPathContainer:", buildPathContainer);
        console.log("templateContainer:", templateContainer);
        console.log("calculationsTemplateContainer:", calculationsTemplateContainer);
    
        if (!datasourceContainer || !searchQueryContainer || !workbookNameContainer ||
            !buildPathContainer || !templateContainer || !calculationsTemplateContainer) {
            console.error("❌ One or more container fields are missing!");
            return;
        }
    
        // Show/Hide Fields
        if (selectedType === "StoredSignal") {
            console.log("🔄 Switching to Stored Signal mode...");
            datasourceContainer.classList.remove("hidden");
            searchQueryContainer.classList.remove("hidden");
            workbookNameContainer.classList.remove("hidden");
            buildPathContainer.classList.remove("hidden");
            templateContainer.classList.remove("hidden"); // ✅ Show Select Template
            calculationsTemplateContainer.classList.add("hidden"); // ❌ Hide Calculations Template
            document.getElementById("templateLabel").innerText = "Select Template:";
        } else if (selectedType === "Calculations") {
            console.log("🔄 Switching to Calculations mode...");
            datasourceContainer.classList.remove("hidden");
            searchQueryContainer.classList.remove("hidden");
            workbookNameContainer.classList.remove("hidden");
            buildPathContainer.classList.remove("hidden");
            templateContainer.classList.remove("hidden"); // ✅ Show Base Template (same field)
            calculationsTemplateContainer.classList.remove("hidden"); // ✅ Show Calculations Template
            document.getElementById("templateLabel").innerText = "Base Template:";
        } else {
            console.warn("⚠️ Unknown type selected, defaulting to Stored Signal.");
        }
    
        console.log("✅ Final Updated Classes:", {
            datasource: datasourceContainer.classList,
            searchQuery: searchQueryContainer.classList,
            workbook: workbookNameContainer.classList,
            buildPath: buildPathContainer.classList,
            template: templateContainer.classList,
            calculationsTemplate: calculationsTemplateContainer.classList
        });
    }

    // ✅ Detect changes to Type selection and apply updates
    typeInput.addEventListener("change", updateFormFields);

    // ✅ Detect changes to Template selection for logging/debugging
    templateSelect.addEventListener("change", () => {
        console.log(`🔄 Switched to template: ${templateSelect.value}`);
    });

    // ✅ Attach Apply Template button click event
    applyTemplateButton.addEventListener("click", applyTemplate);

    // ✅ Run the function at the end to set initial field visibility
    setTimeout(updateFormFields, 200);
});