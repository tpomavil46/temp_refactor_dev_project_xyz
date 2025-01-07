console.log("app.js loaded successfully");

// Track the current tree state
let currentTree = null;

// Handle CSV Upload
document.getElementById("upload-csv").addEventListener("click", async () => {
    const fileInput = document.getElementById("csv-file");
    const formData = new FormData();
    formData.append("file", fileInput.files[0]);

    try {
        const response = await fetch("http://127.0.0.1:8000/upload_csv/", {
            method: "POST",
            body: formData,
        });

        if (!response.ok) {
            throw new Error(`Upload failed with status: ${response.status}`);
        }

        const result = await response.json();
        console.log("File uploaded successfully:", result);
        alert(`File uploaded: ${result.filename}`);
    } catch (error) {
        console.error("Error uploading file:", error);
        alert("Error uploading file. Check the console for details.");
    }
});

// Process csv, prepare, and push the tree
document.getElementById("process-csv").addEventListener("click", async () => {
    const treeNameInput = document.getElementById("tree-name");
    const workbookInput = document.getElementById("workbook-name");

    const treeName = treeNameInput.value.trim();
    const workbookName = workbookInput.value.trim();

    if (!treeName || !workbookName) {
        alert("Please provide both tree name and workbook name.");
        return;
    }

    try {
        const response = await fetch("http://127.0.0.1:8000/process_csv/", {
            method: "POST",
            headers: {
                "Content-Type": "application/json",
            },
            body: JSON.stringify({ tree_name: treeName, workbook_name: workbookName }),
        });

        if (!response.ok) {
            throw new Error(`Processing CSV failed with status: ${response.status}`);
        }

        const result = await response.json();
        console.log("Process CSV response:", result);

        // Update the visualization pane
        const visualizationDiv = document.getElementById("tree-visualization");
        if (result.tree_structure) {
            visualizationDiv.innerHTML = `<pre style="white-space: pre-wrap;">${result.tree_structure}</pre>`;
        } else {
            visualizationDiv.innerHTML = `<p class="placeholder-message">Tree visualization is not available. Please ensure the tree is built and processed correctly.</p>`;
        }

        alert(result.message);
    } catch (error) {
        console.error("Error processing CSV:", error);
        alert("Failed to process the CSV. Check the console for details.");
    }
});

// Push Tree
document.getElementById("push-tree").addEventListener("click", async () => {
    const treeNameInput = document.getElementById("tree-name");
    const workbookInput = document.getElementById("workbook-name");

    const treeName = treeNameInput.value.trim();
    const workbookName = workbookInput.value.trim();

    if (!treeName || !workbookName) {
        alert("Please provide both tree name and workbook name.");
        return;
    }

    try {
        const response = await fetch("http://127.0.0.1:8000/push_tree/", {
            method: "POST",
            headers: {
                "Content-Type": "application/json",
            },
            body: JSON.stringify({ tree_name: treeName, workbook_name: workbookName }),
        });

        if (!response.ok) {
            throw new Error(`Failed to push tree: ${response.status}`);
        }

        const result = await response.json();
        console.log("Push tree response:", result);

        alert(result.message);
    } catch (error) {
        console.error("Error pushing tree:", error);
        alert("Failed to push the tree. Check the console for details.");
    }
});

// Visualize Tree
document.getElementById("visualize-tree").addEventListener("click", async () => {
    try {
        const response = await fetch("http://127.0.0.1:8000/visualize_tree/", {
            method: "GET",
        });

        if (!response.ok) {
            throw new Error(`Visualization failed with status: ${response.status}`);
        }

        const result = await response.json();
        console.log("Visualize Tree response:", result);

        const visualizationDiv = document.getElementById("tree-visualization");
        if (result.tree_structure) {
            visualizationDiv.innerHTML = `<pre style="white-space: pre-wrap;">${result.tree_structure}</pre>`;
        } else {
            visualizationDiv.innerHTML = `<p class="placeholder-message">No tree loaded to visualize.</p>`;
        }
    } catch (error) {
        console.error("Error visualizing tree:", error);
        alert("Failed to visualize the tree. Check the console for details.");
    }
});

// Clear Tree
document.getElementById("clear-tree").addEventListener("click", () => {
    console.log("Clear Tree button clicked."); // Debug log
    const visualizationDiv = document.getElementById("tree-visualization");
    visualizationDiv.innerHTML = `<p class="placeholder-message">Tree visualization has been cleared. Process a CSV or visualize a tree to populate this view.</p>`;
});

// Create Lookup
document.getElementById("create-lookup").addEventListener("click", async () => {
    console.log("Create Lookup button clicked."); // Debug log
    
    try {
        // Example logic for lookup creation (replace with actual API if needed)
        alert("Lookup creation not implemented yet.");
    } catch (error) {
        console.error("Error creating lookup:", error);
        alert("Failed to create lookup. Check the console for details.");
    }
});

// Handle Create Empty Tree
document.getElementById("create-empty-tree").addEventListener("click", async () => {
    const treeNameInput = document.getElementById("tree-name");
    const workbookInput = document.getElementById("workbook-name");

    const treeName = treeNameInput.value.trim();
    const workbookName = workbookInput.value.trim();

    if (!treeName || !workbookName) {
        alert("Please provide both tree name and workbook name.");
        return;
    }

    try {
        const response = await fetch("http://127.0.0.1:8000/create_empty_tree/", {
            method: "POST",
            headers: {
                "Content-Type": "application/json",
            },
            body: JSON.stringify({ tree_name: treeName, workbook_name: workbookName }),
        });

        if (!response.ok) {
            throw new Error(`Failed to create empty tree with status: ${response.status}`);
        }

        const result = await response.json();
        console.log("Empty tree creation response:", result);

        // Update UI with success message and visualization
        alert(result.message);
        const visualizationDiv = document.getElementById("tree-visualization");
        visualizationDiv.innerHTML = `<pre style="white-space: pre-wrap;">${result.tree_structure}</pre>`;
    } catch (error) {
        console.error("Error creating empty tree:", error);
        alert("Failed to create empty tree. Check the console for details.");
    }
});

document.getElementById("search-tree").addEventListener("click", async () => {
    const treeName = document.getElementById("tree-name").value.trim();
    const workbookName = document.getElementById("workbook-name").value.trim();
    if (!treeName || !workbookName) {
        alert("Please provide both a tree name and a workbook name.");
        return;
    }

    try {
        const response = await fetch(`http://127.0.0.1:8000/search_tree/?tree_name=${treeName}&workbook_name=${workbookName}`, {
            method: "GET",
        });

        if (!response.ok) {
            throw new Error(`Search failed with status: ${response.status}`);
        }

        const result = await response.json();
        console.log("Search Tree response:", result);

        const visualizationDiv = document.getElementById("tree-visualization");
        if (result.tree_structure) {
            visualizationDiv.innerHTML = `<pre style="white-space: pre-wrap;">${result.tree_structure}</pre>`;
        } else {
            visualizationDiv.innerHTML = `<p class="placeholder-message">Tree not found. Please check the name and try again.</p>`;
        }
    } catch (error) {
        console.error("Error searching for tree:", error);
        alert("Failed to search for tree. Check the console for details.");
    }
});

// Lookup Workflow ------------------------------------------------------------
// Utility function for toggling visibility
function toggleVisibility(elementId, show) {
    const element = document.getElementById(elementId);
    if (element) {
        element.style.display = show ? "block" : "none";
    }
}

// Upload Raw CSV
document.getElementById("upload-raw-csv").addEventListener("click", async () => {
    const fileInput = document.getElementById("raw-csv");
    const formData = new FormData();
    formData.append("file", fileInput.files[0]);

    try {
        const response = await fetch("http://127.0.0.1:8000/upload_raw_csv/", {
            method: "POST",
            body: formData,
        });

        const result = await response.json();
        alert(result.message);
        toggleVisibility("duplicates-section", true);
    } catch (error) {
        console.error("Error uploading raw CSV:", error);
        alert("Failed to upload raw CSV.");
    }
});

// Utility functions
// Function to populate the duplicates table dynamically
function populateDuplicatesTable(duplicatesData) {
    const tableHeader = document.getElementById("duplicates-table-header");
    const tableBody = document.getElementById("duplicates-table-body");

    // Clear existing table content
    tableHeader.innerHTML = "";
    tableBody.innerHTML = "";

    if (duplicatesData.length === 0) {
        alert("No duplicates found!");
        return;
    }

    // Add headers dynamically
    const headers = Object.keys(duplicatesData[0]);
    headers.forEach((header) => {
        const th = document.createElement("th");
        th.textContent = header;
        tableHeader.appendChild(th);
    });

    // Add a selection column header
    const selectTh = document.createElement("th");
    selectTh.textContent = "Select";
    tableHeader.appendChild(selectTh);

    // Populate rows
    duplicatesData.forEach((row, index) => {
        const tr = document.createElement("tr");

        headers.forEach((header) => {
            const td = document.createElement("td");
            td.textContent = row[header];
            tr.appendChild(td);
        });

        // Add checkbox for selection
        const selectTd = document.createElement("td");
        const checkbox = document.createElement("input");
        checkbox.type = "checkbox";
        checkbox.value = index; // Use index as the value for selection
        selectTd.appendChild(checkbox);
        tr.appendChild(selectTd);

        tableBody.appendChild(tr);
    });

    // Show the duplicates modal
    document.getElementById("duplicates-modal").style.display = "block";
}

// Fetch duplicates and display in modal
document.getElementById("resolve-duplicates").addEventListener("click", async () => {
    const groupColumn = document.getElementById("group-column").value.trim();
    const keyColumn = document.getElementById("key-column").value.trim();
    const valueColumn = document.getElementById("value-column").value.trim();

    if (!groupColumn || !keyColumn || !valueColumn) {
        alert("All fields are required.");
        return;
    }

    const formData = new FormData();
    formData.append("file", document.getElementById("raw-csv").files[0]);
    formData.append("group_column", groupColumn);
    formData.append("key_column", keyColumn);
    formData.append("value_column", valueColumn);

    try {
        const response = await fetch("http://127.0.0.1:8000/duplicates/get_duplicates/", {
            method: "POST",
            body: formData,
        });

        const result = await response.json();
        if (response.ok) {
            alert(result.message);
            populateDuplicatesTable(result.duplicates);
        } else {
            alert(result.detail || "Error fetching duplicates.");
        }
    } catch (error) {
        console.error("Error fetching duplicates:", error);
        alert("Failed to fetch duplicates. Check the console for details.");
    }
});

// Submit selected rows
document.getElementById("submit-selected-rows").addEventListener("click", async () => {
    const selectedRows = Array.from(document.querySelectorAll("#duplicates-table-body input[type='checkbox']:checked")).map(
        (checkbox) => parseInt(checkbox.value)
    );

    if (selectedRows.length === 0) {
        alert("Please select at least one row to keep.");
        return;
    }

    alert("Row selection saved successfully."); // Placeholder for further logic
});

// Set Parent Paths
document.getElementById("set-parent-paths").addEventListener("click", async () => {
    const parentPaths = document.getElementById("parent-paths").value
        .split("\n")
        .reduce((acc, line) => {
            const [group, path] = line.split(":").map((s) => s.trim());
            if (group && path) acc[group] = path;
            return acc;
        }, {});

    try {
        const response = await fetch("http://127.0.0.1:8000/set_parent_paths/", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(parentPaths),
        });

        const result = await response.json();
        alert(result.message);
        toggleVisibility("create-lookup-section", true);
    } catch (error) {
        console.error("Error setting parent paths:", error);
        alert("Failed to set parent paths.");
    }
});

// Generate Lookup
document.getElementById("generate-lookup").addEventListener("click", async () => {
    const outputFile = document.getElementById("output-file").value;

    try {
        const response = await fetch("http://127.0.0.1:8000/generate_lookup/", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ output_file: outputFile }),
        });

        const result = await response.json();
        alert(result.message);
    } catch (error) {
        console.error("Error generating lookup:", error);
        alert("Failed to generate lookup.");
    }
});

document.addEventListener("DOMContentLoaded", () => {
    console.log("DOM fully loaded and parsed.");

    // Add event listener for control selector
    const controlSelector = document.getElementById("control-selector");
    if (controlSelector) {
        controlSelector.addEventListener("change", (event) => {
            console.log("Dropdown value changed to:", event.target.value);

            const assetTreeControls = document.getElementById("asset-tree-controls");
            const lookupControls = document.getElementById("lookup-controls");

            if (assetTreeControls && lookupControls) {
                assetTreeControls.style.display =
                    event.target.value === "asset-tree-controls" ? "block" : "none";
                lookupControls.style.display =
                    event.target.value === "lookup-controls" ? "block" : "none";
                console.log(`Switched to: ${event.target.value}`);
            } else {
                console.error("Control sections not found in the DOM.");
            }
        });
    } else {
        console.error("Control selector not found in the DOM.");
    }

    // Ensure initial visibility state
    const assetTreeControls = document.getElementById("asset-tree-controls");
    const lookupControls = document.getElementById("lookup-controls");
    if (assetTreeControls && lookupControls) {
        assetTreeControls.style.display = "block";
        lookupControls.style.display = "none";
    } else {
        console.error("Control sections not found in the DOM.");
    }
});

document.getElementById("apply-user-specific").addEventListener("click", () => {
    const rowsInput = document.getElementById("rows-to-keep-input").value;
    const rowsToKeep = rowsInput
        .split(",")
        .map((index) => parseInt(index.trim(), 10))
        .filter((index) => !isNaN(index));

    console.log("Rows to keep:", rowsToKeep);
    // Pass rowsToKeep to the `resolve_duplicates` fetch call.
});