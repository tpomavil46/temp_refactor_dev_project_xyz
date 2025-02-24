// src/itv_asset_tree/frontend/accelerator_template_ui.js

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

// ✅ Fetch available hierarchical templates from FastAPI
async function loadHierarchicalTemplates() {
    console.log("🔄 Fetching hierarchical templates...");
    try {
        const response = await fetch("/api/v1/template/templates/hierarchical");
        const data = await response.json();

        if (data.hierarchical_templates && Array.isArray(data.hierarchical_templates)) {
            hierarchicalTemplates = new Set(data.hierarchical_templates);  // ✅ Store templates as a Set
            console.log("✅ Hierarchical templates loaded:", hierarchicalTemplates);
        } else {
            console.warn("⚠️ No hierarchical templates found or invalid response format.");
        }
    } catch (error) {
        console.error("❌ Failed to load hierarchical templates:", error);
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

function populateSignalAssignmentTable(signals) {
    console.log("🔄 Populating signal assignment table...");

    const tableBody = document.getElementById("signalAssignmentBody");
    const componentDropdown = document.getElementById("componentColumnInput");

    if (!tableBody) {
        console.error("❌ Signal assignment table body not found!");
        return;
    }

    tableBody.innerHTML = ""; // Clear existing rows

    if (!signals || signals.length === 0) {
        console.warn("⚠️ No signals available to assign.");
        return;
    }

    signals.forEach(signal => {
        const row = document.createElement("tr");

        const nameCell = document.createElement("td");
        nameCell.textContent = signal; // ✅ Signal name
        row.appendChild(nameCell);

        const componentCell = document.createElement("td");
        const select = document.createElement("select");

        // ✅ Populate dropdown with component options
        if (componentDropdown) {
            Array.from(componentDropdown.options).forEach(option => {
                const opt = document.createElement("option");
                opt.value = option.value;
                opt.textContent = option.textContent;
                select.appendChild(opt);
            });
        }

        componentCell.appendChild(select);
        row.appendChild(componentCell);

        tableBody.appendChild(row);
    });

    document.getElementById("signalAssignmentContainer").classList.remove("hidden"); // ✅ Show table
    console.log("✅ Signal assignment table populated.");
}

async function fetchAvailableTags() {
    const searchQuery = document.getElementById("searchQueryInput").value.trim();
    const datasourceName = document.getElementById("datasourceInput").value.trim();

    if (!searchQuery || !datasourceName) {
        alert("⚠️ Please enter a search query and datasource name.");
        return;
    }

    console.log(`🔍 Fetching available signals for query: ${searchQuery}`);

    try {
        const response = await fetch(`/api/v1/template/fetch_signals?search_query=${encodeURIComponent(searchQuery)}&datasource_name=${encodeURIComponent(datasourceName)}`);
        const data = await response.json();

        if (response.ok && data.signals && Array.isArray(data.signals) && data.signals.length > 0) {
            console.log("✅ Signals retrieved:", data.signals);

            // Populate the assignment table with signal names
            populateSignalAssignmentTable(data.signals);

        } else {
            console.error("❌ Failed to fetch signals:", data.detail || "No signals found.");
            alert(`❌ Error: ${data.detail || "No signals found."}`);
        }
    } catch (error) {
        console.error("❌ Error fetching signals:", error);
    }
}

async function fetchAvailableComponents() {
    console.log("🔄 Fetching available components...");
    try {
        const response = await fetch("/api/v1/template/fetch_components");
        const data = await response.json();

        // Ensure components exist, otherwise use fallback values
        if (!data.components || !Array.isArray(data.components) || data.components.length === 0) {
            console.warn("⚠️ No components found in API response, using fallback values.");
            data.components = ["Refrigerator", "Compressor", "Motor", "Pump"];
        } else {
            console.log("✅ Components retrieved:", data.components);
        }

        // ✅ Populate the "Select Components" dropdown
        const componentDropdown = document.getElementById("componentColumnInput");
        if (!componentDropdown) {
            console.error("❌ Component selection dropdown not found in DOM!");
            return;
        }

        // Clear existing options
        componentDropdown.innerHTML = "";

        // Populate dropdown with components
        data.components.forEach(component => {
            const option = document.createElement("option");
            option.value = component;
            option.textContent = component;
            componentDropdown.appendChild(option);
        });

        // ✅ Ensure the component selection field is visible
        componentDropdown.classList.remove("hidden");
        document.getElementById("hierarchicalAssetsContainer").classList.remove("hidden");

        // ✅ Populate the signal assignment table
        const signalAssignmentBody = document.getElementById("signalAssignmentBody");
        if (!signalAssignmentBody) {
            console.error("❌ Signal assignment table body not found in DOM!");
            return;
        }

        // Clear existing table rows
        signalAssignmentBody.innerHTML = "";

        // Populate table rows with dropdowns for each component
        data.components.forEach(component => {
            const row = document.createElement("tr");

            const signalCell = document.createElement("td");
            signalCell.textContent = `Assign to ${component}`;

            const selectCell = document.createElement("td");
            const selectDropdown = document.createElement("select");

            // Populate dropdown with all components
            data.components.forEach(opt => {
                const option = document.createElement("option");
                option.value = opt;
                option.textContent = opt;
                selectDropdown.appendChild(option);
            });

            selectCell.appendChild(selectDropdown);
            row.appendChild(signalCell);
            row.appendChild(selectCell);
            signalAssignmentBody.appendChild(row);
        });

        // ✅ Ensure the assignment table is visible
        document.getElementById("signalAssignmentContainer").classList.remove("hidden");

    } catch (error) {
        console.error("❌ Failed to fetch components:", error);
    }
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

document.addEventListener("DOMContentLoaded", async () => {
    console.log("✅ Document Loaded.");

    await fetchAvailableComponents();  // ✅ Auto-fetch components
    await loadHierarchicalTemplates(); // ✅ Fetch hierarchical templates dynamically

    // ✅ Ensure the button and dropdown exist before adding event listeners
    const fetchTagsButton = document.getElementById("fetchTagsButton");
    const componentDropdown = document.getElementById("componentColumnInput");
    const hierarchicalAssetsContainer = document.getElementById("hierarchicalAssetsContainer");

    if (fetchTagsButton) {
        fetchTagsButton.addEventListener("click", fetchAvailableTags);
        fetchTagsButton.classList.remove("hidden");  // ✅ Ensure it's visible
        console.log("✅ Fetch Tags button is now visible.");
    } else {
        console.error("❌ Fetch Tags button not found in DOM!");
    }

    if (componentDropdown) {
        componentDropdown.classList.remove("hidden");  // ✅ Ensure the components dropdown is visible
        console.log("✅ Component selection field is now visible.");
    } else {
        console.error("❌ Component selection dropdown not found in DOM!");
    }

    if (hierarchicalAssetsContainer) {
        hierarchicalAssetsContainer.classList.remove("hidden");  // ✅ Ensure hierarchical section is visible
        console.log("✅ Hierarchical Assets section is now visible.");
    } else {
        console.error("❌ Hierarchical Assets section not found in DOM!");
    }

    document.getElementById("templateSelect").addEventListener("change", updateFormFields);
    document.getElementById("applyTemplateButton").addEventListener("click", applyTemplate);
    document.getElementById("typeInput").addEventListener("change", updateFormFields);

    setTimeout(updateFormFields, 200);
});

// ✅ Store hierarchical templates dynamically
let hierarchicalTemplates = new Set();

function updateFormFields() {
    console.log("🛠 Running updateFormFields...");

    const selectedTemplate = document.getElementById("templateSelect").value.trim();
    const hierarchicalAssetsContainer = document.getElementById("hierarchicalAssetsContainer");
    const componentDropdown = document.getElementById("componentColumnInput");
    const fetchTagsButton = document.getElementById("fetchTagsButton");

    // ✅ Always show Fetch Tags Button
    fetchTagsButton.classList.remove("hidden");

    if (hierarchicalTemplates.has(selectedTemplate)) {
        hierarchicalAssetsContainer.classList.remove("hidden");
        componentDropdown.classList.remove("hidden"); // ✅ Ensure the Components Selection Field is visible
        console.log(`✅ Showing hierarchical fields for template: ${selectedTemplate}`);

        // ✅ Auto-fetch components when a hierarchical template is selected
        fetchAvailableComponents();
    } else {
        hierarchicalAssetsContainer.classList.add("hidden");
        componentDropdown.classList.add("hidden"); // ✅ Hide Components Selection Field if not hierarchical
        console.log(`🚫 Hiding hierarchical fields for non-hierarchical template: ${selectedTemplate}`);
    }
}