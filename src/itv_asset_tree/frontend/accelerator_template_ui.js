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

function populateSignalAssignmentTable(signals, components) {
    console.log("🔄 Populating signal assignment table...");

    const tableBody = document.getElementById("signalAssignmentBody");

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
        nameCell.textContent = signal;
        row.appendChild(nameCell);

        const componentCell = document.createElement("td");
        const select = document.createElement("select");

        // ✅ Populate dropdown with cached component options
        components.forEach(component => {
            const opt = document.createElement("option");
            opt.value = component;
            opt.textContent = component;
            select.appendChild(opt);
        });

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

            // ✅ Use cached components to prevent dropdown wipeout
            populateSignalAssignmentTable(data.signals, cachedComponents);

        } else {
            console.error("❌ Failed to fetch signals:", data.detail || "No signals found.");
            alert(`❌ Error: ${data.detail || "No signals found."}`);
        }
    } catch (error) {
        console.error("❌ Error fetching signals:", error);
    }
}

let cachedComponents = [];  // ✅ Store components globally

async function fetchAvailableComponents() {
    console.log("🔄 Fetching available components...");
    try {
        const response = await fetch("/api/v1/template/fetch_components");
        const data = await response.json();

        if (!data.components || !Array.isArray(data.components) || data.components.length === 0) {
            console.warn("⚠️ No components found in API response, using fallback values.");
            cachedComponents = ["Refrigerator", "Compressor", "Motor", "Pump"];
        } else {
            console.log("✅ Components retrieved:", data.components);
            cachedComponents = data.components;
        }

        console.log("📌 Cached components:", cachedComponents);

    } catch (error) {
        console.error("❌ Failed to fetch components:", error);
    }
}

async function applyTemplate() {
    console.log("🚀 Applying template...");

    const templateSelect = document.getElementById('templateSelect');
    const typeInput = document.getElementById('typeInput');
    const calculationsTemplateInput = document.getElementById('calculationsTemplateInput');
    const metricsTemplateInput = document.getElementById('metricsTemplateInput');

    if (!templateSelect || !typeInput) {
        console.error("❌ Missing templateSelect or typeInput in the DOM!");
        return;
    }

    const searchQuery = document.getElementById('searchQueryInput')?.value.trim() || "";
    const datasourceName = document.getElementById('datasourceInput')?.value.trim() || "";
    const workbookName = document.getElementById('workbookNameInput')?.value.trim() || "";
    const buildPath = document.getElementById('buildPathInput')?.value.trim() || "";
    const baseTemplate = templateSelect.value;

    console.log("🔍 Entered Search Query:", searchQuery); // ✅ Debugging Line

    // Check if searchQuery is set correctly
    if (!searchQuery) {
        alert("⚠️ Search Query is required!");
        return;
    }

    let payload = {
        template_name: templateSelect.value,
        type: getSeeqType(typeInput.options[typeInput.selectedIndex]?.value || "", templateSelect.value),
        build_asset_regex: searchQuery,  // ✅ Use user-inputted search query dynamically
        build_path: buildPath || "My HVAC Units >> Facility #1",
        workbook_name: workbookName,
        search_query: searchQuery, // ✅ Make sure this is sent properly
        base_template: baseTemplate,
        calculations_template: calculationsTemplateInput ? calculationsTemplateInput.value.trim() : "",
        metrics_template: metricsTemplateInput ? metricsTemplateInput.value.trim() : "",
        datasource_name: datasourceName
    };

    console.log("📌 Final Payload Before Sending:", JSON.stringify(payload, null, 2)); // ✅ Debugging Line

    let apiEndpoint = "/api/v1/template/build";  // Default for Stored Signals

    if (payload.type.includes("Calculated")) {
        apiEndpoint = "/api/v1/template/build_calculated";
    } else if (payload.type.includes("Metric")) {
        apiEndpoint = "/api/v1/template/build_metrics";
    }

    try {
        console.log("🔍 Sending request to:", apiEndpoint);

        const response = await fetch(apiEndpoint, {
            method: 'POST',
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(payload)
        });

        const result = await response.json();

        if (response.ok) {
            console.log("✅ Template applied successfully:", result);
        } else {
            console.error("❌ Failed to apply template:", result);
            alert(`❌ Error: ${result.detail}`);
        }
    } catch (error) {
        console.error("❌ Request failed:", error);
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

    const selectedType = document.getElementById("typeInput").value.trim();
    const calculationsTemplateContainer = document.getElementById("calculationsTemplateContainer");
    const metricsTemplateContainer = document.getElementById("metricsTemplateContainer");  // ✅ NEW
    const templateContainer = document.getElementById("templateContainer");
    const templateLabel = document.getElementById("templateLabel");

    if (!calculationsTemplateContainer || !metricsTemplateContainer || !templateContainer || !templateLabel) {
        console.error("❌ Missing required elements in the DOM for template visibility.");
        return;
    }

    if (selectedType === "StoredSignal") {
        console.log("🔄 Switching to Stored Signal mode...");
        calculationsTemplateContainer.classList.add("hidden");
        metricsTemplateContainer.classList.add("hidden");
        templateContainer.classList.remove("hidden");
        templateLabel.innerText = "Select Template:";
    } else if (selectedType === "Calculations") {
        console.log("🔄 Switching to Calculations mode...");
        calculationsTemplateContainer.classList.remove("hidden");
        metricsTemplateContainer.classList.add("hidden");
        templateContainer.classList.remove("hidden");
        templateLabel.innerText = "Base Template:";
    } else if (selectedType === "Metric") {
        console.log("🔄 Switching to Metrics mode...");
        calculationsTemplateContainer.classList.add("hidden");
        metricsTemplateContainer.classList.remove("hidden");
        templateContainer.classList.remove("hidden");
        templateLabel.innerText = "Base Template:";
    } else {
        console.warn("⚠️ Unknown type selected, defaulting to Stored Signal.");
    }

    console.log("✅ Final Updated Classes:", {
        calculationsTemplateContainer: calculationsTemplateContainer.classList,
        metricsTemplateContainer: metricsTemplateContainer.classList,
        templateContainer: templateContainer.classList,
    });
}