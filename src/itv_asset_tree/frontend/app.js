// Track the current tree state
let currentTree = null;

//////////////////////////////////////////////////////////////////////////////////////////////
//                                   🔹 UTILITY FUNCTIONS 🔹                                //
//////////////////////////////////////////////////////////////////////////////////////////////

/** HELPER FUNCTION: General function to send a POST request */
async function sendPostRequest(url, data) {
    const spinner = document.getElementById("loading-spinner");

    try {
        if (spinner) spinner.style.display = "block";
        const response = await fetch(url, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(data),
        });

        if (!response.ok) {
            throw new Error(`❌ Request failed with status: ${response.status}`);
        }

        return await response.json();
    } catch (error) {
        console.error("❌ API Error:", error);
        alert(`⚠️ Error: ${error.message}`);
    } finally {
        // Hide the spinner after request finishes
        if (spinner) spinner.style.display = "none";
    }
}

/** Toggle visibility of an element */
function toggleVisibility(elementId, show) {
    const element = document.getElementById(elementId);
    if (element) {
        element.style.display = show ? "block" : "none";
    }
}

/** Fetch lookup names */
async function fetchLookupNames() {
    try {
        const response = await fetch("http://127.0.0.1:8000/fetch_lookup_names/");
        if (!response.ok) {
            throw new Error("❌ Failed to fetch lookup string names.");
        }
        const data = await response.json();
        return data.names || [];
    } catch (error) {
        console.error("❌ Error fetching lookup names:", error);
        alert("⚠️ Failed to fetch lookup string names.");
        return [];
    }
}

/** Populate duplicates table */
function populateDuplicatesTable(duplicatesData) {
    const tableHeader = document.getElementById("duplicates-table-header");
    const tableBody = document.getElementById("duplicates-table-body");

    tableHeader.innerHTML = "";
    tableBody.innerHTML = "";

    if (duplicatesData.length === 0) {
        alert("No duplicates found!");
        return;
    }

    const headers = Object.keys(duplicatesData[0]);
    headers.forEach((header) => {
        const th = document.createElement("th");
        th.textContent = header;
        tableHeader.appendChild(th);
    });

    const selectTh = document.createElement("th");
    selectTh.textContent = "Select";
    tableHeader.appendChild(selectTh);

    duplicatesData.forEach((row, index) => {
        const tr = document.createElement("tr");

        headers.forEach((header) => {
            const td = document.createElement("td");
            td.textContent = row[header];
            tr.appendChild(td);
        });

        const selectTd = document.createElement("td");
        const checkbox = document.createElement("input");
        checkbox.type = "checkbox";
        checkbox.value = index;
        selectTd.appendChild(checkbox);
        tr.appendChild(selectTd);

        tableBody.appendChild(tr);
    });

    document.getElementById("duplicates-modal").style.display = "block";
}

/** SUBMIT SELECTED ROWS */
document.getElementById("submit-selected-rows").addEventListener("click", async () => {
    console.log("🚀 Submit Selected Rows button clicked!");

    const selectedRows = Array.from(
        document.querySelectorAll("#duplicates-table-body input[type='checkbox']:checked")
    ).map((checkbox) => parseInt(checkbox.value));

    if (selectedRows.length === 0) {
        alert("⚠️ No rows selected. Keeping all rows.");
    }

    const groupColumn = document.getElementById("group-column").value.trim();
    const keyColumn = document.getElementById("key-column").value.trim();
    const valueColumn = document.getElementById("value-column").value.trim();

    const rawCsvFile = document.getElementById("raw-csv").files[0];
    if (!rawCsvFile || !groupColumn || !keyColumn || !valueColumn) {
        alert("⚠️ All fields are required, and a file must be uploaded.");
        return;
    }

    const formData = new FormData();
    formData.append("file", rawCsvFile);
    formData.append("group_column", groupColumn);
    formData.append("key_column", keyColumn);
    formData.append("value_column", valueColumn);
    formData.append("rows_to_remove", JSON.stringify(selectedRows));

    try {
        console.log("📡 Sending request to resolve duplicates...");
        const response = await fetch("http://127.0.0.1:8000/duplicates/resolve_duplicates/", {
            method: "POST",
            body: formData,
        });

        if (!response.ok) {
            throw new Error(`❌ Resolving duplicates failed with status: ${response.status}`);
        }

        const result = await response.json();
        console.log("✅ Resolve Duplicates response:", result);

        alert(result.message);

        // Display Parent Path Inputs
        await displayParentPathInputs(); // Ensure the parent paths section is populated
        console.log("✅ Parent Paths section prepared and displayed.");
    } catch (error) {
        console.error("❌ Error resolving duplicates:", error);
        alert("⚠️ Failed to resolve duplicates. Check the console for details.");
    }
});

/** DISPLAY PARENT PATH INPUTS */
async function displayParentPathInputs() {
    try {
        console.log("📡 Fetching lookup string names for Parent Paths...");
        const response = await fetch("http://127.0.0.1:8000/duplicates/names/");

        if (!response.ok) {
            throw new Error(`❌ Failed to fetch lookup string names. Status: ${response.status}`);
        }

        const { lookup_names } = await response.json();
        if (lookup_names.length === 0) {
            alert("⚠️ No lookup strings available. Ensure duplicates are resolved first.");
            return;
        }

        console.log("✅ Lookup names received:", lookup_names);

        const parentPathSection = document.getElementById("parent-path-section");
        parentPathSection.style.display = "block"; // Ensure section is visible
        const parentPathsDiv = document.getElementById("parent-paths-section");
        parentPathsDiv.innerHTML = ""; // Clear existing fields

        lookup_names.forEach((name) => {
            const label = document.createElement("label");
            label.textContent = `Parent Path for ${name}:`;
            const input = document.createElement("input");
            input.type = "text";
            input.id = `parent-path-${name}`;
            input.placeholder = `Enter Parent Path for ${name}`;
            parentPathsDiv.appendChild(label);
            parentPathsDiv.appendChild(input);
        });

        // Ensure the "Set Parent Paths" button is visible
        const setParentPathsButton = document.getElementById("set-parent-paths");
        if (setParentPathsButton) {
            setParentPathsButton.style.display = "inline-block";
        }
    } catch (error) {
        console.error("❌ Error displaying Parent Path inputs:", error);
        alert("⚠️ Failed to fetch lookup string names. Check the console for details.");
    }
}

/** CREATE EMPTY TREE */
function attachCreateTreeListener() {
    const createTreeButton = document.getElementById("create-empty-tree");
    if (!createTreeButton) return;

    createTreeButton.addEventListener("click", async () => {
        console.log("🚀 Create Empty Tree button clicked!");
        const treeName = document.getElementById("tree-name").value.trim();
        const workbookName = document.getElementById("workbook-name").value.trim();

        if (!treeName || !workbookName) {
            alert("⚠️ Please provide both tree name and workbook name.");
            return;
        }

        const result = await sendPostRequest("http://127.0.0.1:8000/create_empty_tree/", {
            tree_name: treeName,
            workbook_name: workbookName,
        });

        if (result) {
            alert(result.message);
            document.getElementById("tree-visualization").innerHTML = 
                `<pre style="white-space: pre-wrap;">${result.tree_structure || "No tree structure available"}</pre>`;
        }
    });
}

/** UPLOAD CSV */
function attachCsvUploadListener() {
    document.getElementById("upload-csv").addEventListener("click", async () => {
        const fileInput = document.getElementById("csv-file");
        if (!fileInput.files.length) {
            alert("⚠️ Please select a file to upload.");
            return;
        }

        const formData = new FormData();
        formData.append("file", fileInput.files[0]);

        try {
            const response = await fetch("http://127.0.0.1:8000/upload_csv/", { method: "POST", body: formData });
            const result = await response.json();
            alert(`File uploaded: ${result.filename}`);
        } catch (error) {
            console.error("❌ Error uploading file:", error);
        }
    });
}

/** PROCESS CSV */
function attachProcessCsvListener() {
    document.getElementById("process-csv").addEventListener("click", async () => {
        const treeName = document.getElementById("tree-name").value.trim();
        const workbookName = document.getElementById("workbook-name").value.trim();

        if (!treeName || !workbookName) {
            alert("⚠️ Please provide both tree name and workbook name.");
            return;
        }

        const result = await sendPostRequest("http://127.0.0.1:8000/process_csv/", { tree_name: treeName, workbook_name: workbookName });
        if (result) {
            alert(result.message);
            document.getElementById("tree-visualization").innerHTML = 
                `<pre style="white-space: pre-wrap;">${result.tree_structure || "No tree structure available"}</pre>`;
        }
    });
}

/** PUSH TREE */
function attachPushTreeListener() {
    const pushTreeButton = document.getElementById("push-tree");

    if (!pushTreeButton) {
        console.warn("⚠️ Push Tree button not found in the DOM. Skipping listener attachment.");
        return; // 🚀 Prevents errors from missing elements
    }

    console.log("✅ Push Tree button listener attached.");

    pushTreeButton.addEventListener("click", async () => {
        console.log("🚀 Push Tree button clicked!");

        const treeName = document.getElementById("tree-name")?.value.trim();
        const workbookName = document.getElementById("workbook-name")?.value.trim();

        if (!treeName || !workbookName) {
            alert("⚠️ Please provide both tree name and workbook name.");
            return;
        }

        console.log(`📡 Pushing Tree: '${treeName}' in Workbook: '${workbookName}'...`);

        try {
            const response = await fetch("http://127.0.0.1:8000/push_tree/", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ tree_name: treeName, workbook_name: workbookName }),
            });

            if (!response.ok) {
                throw new Error(`Push request failed with status: ${response.status}`);
            }

            const result = await response.json();
            console.log("✅ Push response:", result);
            alert(result.message);

            // ✅ Update visualization after pushing the tree
            console.log("📡 Updating tree visualization...");
            await updateTreeVisualization(treeName, workbookName);
        } catch (error) {
            console.error("❌ Error pushing tree:", error);
            alert("Failed to push the tree. Check the console for details.");
        }
    });
}

/** TREE VISUALIZATION */
async function updateTreeVisualization(treeName, workbookName) {
    console.log(`📡 [DEBUG] updateTreeVisualization() CALLED for Tree: '${treeName}', Workbook: '${workbookName}'`);
    const loadingSpinner = document.getElementById("loading-spinner");

    if (!treeName || !workbookName) {
        console.error("❌ Tree Name or Workbook Name is missing! Cannot update visualization.");
        return;
    }

    try {
        loadingSpinner.style.display = "block";
        // Force fresh response by adding timestamp
        const timestamp = new Date().getTime();
        const response = await fetch(`http://127.0.0.1:8000/visualize_tree/?tree_name=${treeName}&workbook_name=${workbookName}&_=${timestamp}`);

        if (!response.ok) {
            throw new Error(`Visualization request failed with status: ${response.status}`);
        }

        const result = await response.json();
        console.log("✅ Latest Visualization:", result);

        document.getElementById("tree-visualization").innerHTML = 
            `<pre style="white-space: pre-wrap;">${result.tree_structure || "No tree available"}</pre>`;
    } catch (error) {
        console.error("❌ Error updating tree visualization:", error);
        alert("⚠️ Failed to update tree visualization.");
    } finally {
        loadingSpinner.style.display = "none";  // Hide spinner when done
    }
}

/** VISUALIZE TREE */
function attachVisualizeTreeListener() {
    const visualizeTreeButton = document.getElementById("visualize-tree");

    if (!visualizeTreeButton) {
        console.warn("⚠️ Visualize Tree button not found in the DOM. Skipping listener attachment.");
        return; // 🚀 Prevents errors from missing elements
    }

    console.log("✅ Visualize Tree button listener attached.");

    visualizeTreeButton.addEventListener("click", async () => {
        console.log("🚀 Visualize Tree button clicked!");

        try {
            const response = await fetch("http://127.0.0.1:8000/visualize_tree/");
            if (!response.ok) throw new Error(`❌ Failed with status: ${response.status}`);

            const result = await response.json();
            console.log("✅ Visualization response received:", result);

            const treeVisualizationElement = document.getElementById("tree-visualization");
            if (treeVisualizationElement) {
                treeVisualizationElement.innerHTML = 
                    `<pre style="white-space: pre-wrap;">${result.tree_structure || "No tree available"}</pre>`;
            } else {
                console.warn("⚠️ 'tree-visualization' element not found in the DOM.");
            }
        } catch (error) {
            console.error("❌ Error visualizing tree:", error);
            alert("⚠️ Failed to visualize the tree. Check console for details.");
        }
    });
}

/** VISUALIZE TREE */
function attachVisualizeTreeListener() {
    const visualizeTreeButton = document.getElementById("visualize-tree");

    // ✅ Exit early if button does NOT exist (prevents error)
    if (!visualizeTreeButton) return;

    console.log("✅ Visualize Tree button listener attached.");

    visualizeTreeButton.addEventListener("click", async () => {
        console.log("🚀 Visualize Tree button clicked!");

        try {
            const response = await fetch("http://127.0.0.1:8000/visualize_tree/");
            if (!response.ok) throw new Error(`❌ Failed with status: ${response.status}`);

            const result = await response.json();
            console.log("✅ Visualization response received:", result);

            const treeVisualizationElement = document.getElementById("tree-visualization");
            if (treeVisualizationElement) {
                treeVisualizationElement.innerHTML = 
                    `<pre style="white-space: pre-wrap;">${result.tree_structure || "No tree available"}</pre>`;
            } else {
                console.warn("⚠️ 'tree-visualization' element not found in the DOM.");
            }
        } catch (error) {
            console.error("❌ Error visualizing tree:", error);
            alert("⚠️ Failed to visualize the tree. Check console for details.");
        }
    });
}

/** CLEAR TREE */
function attachClearTreeListener() {
    document.getElementById("clear-tree").addEventListener("click", () => {
        console.log("Clear Tree button clicked.");
        document.getElementById("tree-visualization").innerHTML = `<p class="placeholder-message">Tree visualization cleared.</p>`;
    });
}

/** GENERATE LOOKUP TABLE */
function attachGenerateLookupListener() {
    const generateLookupButton = document.getElementById("generate-lookup-btn");
    if (!generateLookupButton) {
        console.warn("⏳ Generate Lookup button not found. Retrying...");
        setTimeout(attachGenerateLookupListener, 300000); // Retry after 5 minutes
        return;
    }

    console.log("✅ Generate Lookup listener attached.");
    generateLookupButton.addEventListener("click", async () => {
        console.log("🚀 Generate Lookup button clicked!");

        const outputFileNameInput = document.getElementById("lookup-output-file");
        const outputFileName = outputFileNameInput ? outputFileNameInput.value.trim() : "";

        if (!outputFileName) {
            alert("⚠️ Please provide a valid output file name.");
            return;
        }

        try {
            console.log("📡 Sending request to generate lookup table...");
            const response = await fetch("http://127.0.0.1:8000/duplicates/generate_lookup/", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ output_file_name: outputFileName }),
            });

            if (!response.ok) {
                throw new Error(`❌ Generate Lookup failed with status: ${response.status}`);
            }

            const result = await response.json();
            console.log("✅ Generate Lookup response:", result);

            alert(result.message);

            // Additional logic for success (if needed)
        } catch (error) {
            console.error("❌ Error generating lookup table:", error);
            alert("⚠️ Failed to generate the lookup table. Check the console for details.");
        }
    });
}

/** SEARCH TREE */
function attachSearchTreeListener() {
    document.getElementById("search-tree").addEventListener("click", async () => {
        const treeName = document.getElementById("tree-name").value.trim();
        const workbookName = document.getElementById("workbook-name").value.trim();

        if (!treeName || !workbookName) {
            alert("⚠️ Please provide both a tree name and a workbook name.");
            return;
        }

        try {
            const response = await fetch(`http://127.0.0.1:8000/search_tree/?tree_name=${treeName}&workbook_name=${workbookName}`);
            if (!response.ok) throw new Error(`❌ Search failed with status: ${response.status}`);
            const result = await response.json();
            document.getElementById("tree-visualization").innerHTML = 
                `<pre style="white-space: pre-wrap;">${result.tree_structure || "Tree not found"}</pre>`;
        } catch (error) {
            console.error("Error searching for tree:", error);
            alert("⚠️ Failed to search for tree.");
        }
    });
}

/** INSERT ITEM */
function attachInsertItemListener() {
    const insertItemButton = document.getElementById("insert-item");
    if (!insertItemButton) return;

    insertItemButton.addEventListener("click", async () => {
        console.log("🚀 Insert Item button clicked!");
        
        const treeName = document.getElementById("tree-name").value.trim();
        const workbookName = document.getElementById("workbook-name").value.trim();
        const fileInput = document.getElementById("csv-file");

        if (!treeName || !workbookName) {
            alert("⚠️ Please provide both tree name and workbook name.");
            return;
        }

        if (!fileInput.files.length) {
            alert("⤴️ Please upload a CSV file.");
            return;
        }

        const formData = new FormData();
        formData.append("file", fileInput.files[0]);
        formData.append("tree_name", treeName);
        formData.append("workbook_name", workbookName);

        try {
            const response = await fetch("http://127.0.0.1:8000/modify_tree/", {
                method: "POST",
                body: formData,
            });

            const result = await response.json();
            if (response.ok) {
                alert(result.message);
            } else {
                alert(result.detail || "⚠️ Failed to modify the tree.");
            }
        } catch (error) {
            console.error("❌ Error modifying the tree:", error);
            alert("⚠️ An error occurred while modifying the tree.");
        }
    });
}

async function insertItem() {
    const treeName = document.getElementById("tree-name").value.trim();
    const workbookName = document.getElementById("workbook-name").value.trim();
    const parentPath = document.getElementById("parentPath").value.trim();
    const itemName = document.getElementById("name").value.trim();
    const itemType = document.getElementById("item-type").value.trim();
    const formula = document.getElementById("formula").value.trim();
    const formulaParams = document.getElementById("formulaParams").value.trim();
    const loadingSpinner = document.getElementById("loading-spinner");

    if (!treeName || !workbookName || !parentPath || !itemName || !itemType) {
        alert("⚠️ Please provide Tree Name, Workbook Name, Parent Path, Name, and Type.");
        return;
    }

    // Ensure proper formatting for formula parameters
    let parsedFormulaParams = {};
    if (formulaParams) {
        try {
            parsedFormulaParams = JSON.parse(formulaParams);
        } catch (error) {
            alert("⚠️ Invalid JSON format in Formula Parameters.");
            console.error("❌ Invalid JSON in FormulaParams:", error);
            return;
        }
    }

    // Prepare the item definition properly
    const itemDefinition = {
        Name: itemName,
        Type: itemType,
        Formula: formula || null,
        FormulaParams: parsedFormulaParams // Ensure valid JSON object
    };

    const requestData = {
        tree_name: treeName,
        workbook_name: workbookName,
        parent_name: parentPath,
        item_definition: itemDefinition
    };

    try {
        loadingSpinner.style.display = "block";
        console.log(`📡 Sending insert request: ${JSON.stringify(requestData)}`);

        const response = await fetch("http://127.0.0.1:8000/insert_item/", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(requestData),
        });

        if (!response.ok) {
            const errorData = await response.json();
            throw new Error(`❌ Insert failed: ${errorData.detail || response.statusText}`);
        }

        const result = await response.json();
        alert(result.message);
        console.log("✅ Insert successful:", result);

        // Refresh tree visualization after insertion
        await updateTreeVisualization(treeName, workbookName);

    } catch (error) {
        console.error("❌ Error inserting item:", error);
        alert("⚠️ Failed to insert item. Check the console for details.");
    } finally {
        loadingSpinner.style.display = "none";  // ✅ Hide spinner when done
    }
}

async function removeItem() {
    const itemPath = document.getElementById("removePath").value.trim();
    const loadingSpinner = document.getElementById("loading-spinner");

    if (!itemPath) {
        alert("⚠️ Please provide the full path of the item to remove.");
        return;
    }

    const requestData = {
        tree_name: document.getElementById("tree-name").value.trim(),
        workbook_name: document.getElementById("workbook-name").value.trim(),
        item_path: itemPath
    };

    try {
        loadingSpinner.style.display = "block";  
        console.log(`📡 Sending remove request: ${JSON.stringify(requestData)}`);

        const response = await fetch("http://127.0.0.1:8000/remove_item/", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(requestData),
        });

        if (!response.ok) {
            throw new Error(`❌ Remove failed: ${response.statusText}`);
        }

        const result = await response.json();
        alert(result.message);
        console.log("✅ Remove successful:", result);

        // Refresh tree visualization
        await updateTreeVisualization(requestData.tree_name, requestData.workbook_name);

    } catch (error) {
        console.error("❌ Error removing item:", error);
        alert("⚠️ Failed to remove item. Check the console for details.");
    } finally {
        loadingSpinner.style.display = "none";  // ✅ Hide spinner when done
    }
}

/** ATTACH MODIFY TREE LISTENER */
function attachModifyTreeListener() {
    const modifyDialog = document.getElementById("modifyDialog");
    const modifyButton = document.getElementById("openModifyDialog");
    const closeButton = document.querySelector(".modal .close");
    const operationSelect = document.getElementById("operation");
    const insertFields = document.getElementById("insertFields");
    const moveFields = document.getElementById("moveFields");
    const removeFields = document.getElementById("removeFields");
    const submitButton = document.getElementById("submitModification");

    if (!modifyDialog || !modifyButton) {
        console.error("❌ Modify Dialog or Button not found in DOM!");
        return;
    }

    // Show modal when "Modify Tree" button is clicked
    modifyButton.addEventListener("click", () => {
        modifyDialog.style.display = "block";
    });

    // Close modal when "X" is clicked
    closeButton.addEventListener("click", () => {
        modifyDialog.style.display = "none";
    });

    // Hide modal when clicking outside of it
    window.addEventListener("click", (event) => {
        if (event.target === modifyDialog) {
            modifyDialog.style.display = "none";
        }
    });

    // Toggle input fields based on selected operation
    operationSelect.addEventListener("change", (event) => {
        const selectedOperation = event.target.value;
        insertFields.style.display = selectedOperation === "insert" ? "block" : "none";
        moveFields.style.display = selectedOperation === "move" ? "block" : "none";
        removeFields.style.display = selectedOperation === "remove" ? "block" : "none";
    });

    // Handle form submission
    submitButton.addEventListener("click", async () => {
        const operation = operationSelect.value.trim();
    
        if (!operation) {
            alert("⚠️ Please select an operation.");
            return;
        }
    
        // Call `insertItem()` if inserting
        if (operation === "insert") {
            await insertItem(); 
            return;
        }
    
        const treeName = document.getElementById("tree-name").value.trim();
        const workbookName = document.getElementById("workbook-name").value.trim();
    
        if (!treeName || !workbookName) {
            alert("⚠️ Please provide both tree name and workbook name.");
            return;
        }
    
        let requestData = { tree_name: treeName, workbook_name: workbookName };
    
        // Handle Move
        if (operation === "move") {
            requestData.source_path = document.getElementById("sourcePath").value.trim();
            requestData.destination_path = document.getElementById("destinationPath").value.trim();
        } 
        // Handle Remove
        else if (operation === "remove") {
            const removePathInput = document.getElementById("removePath");
        
            if (!removePathInput) {
                console.error("❌ ERROR: Element with ID 'removePath' not found in the DOM.");
                alert("⚠️ Please enter a valid path to remove.");
                return;
            }
        
            const itemPath = removePathInput.value.trim();
            requestData.item_path = itemPath;  // ✅ Fix: Ensure correct key
        }
    
        // Determine API endpoint
        let endpoint = "";
        if (operation === "move") endpoint = "/move_item/";
        else if (operation === "remove") endpoint = "/remove_item/";
    
        console.log("📤 Sending request:", JSON.stringify(requestData));
    
        try {
            const response = await fetch(endpoint, {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify(requestData),
            });
    
            const data = await response.json();
            console.log("📥 Received response:", data);
            alert(data.message || "Operation completed successfully.");
            modifyDialog.style.display = "none";
    
            // Refresh tree visualization after modification
            console.log("📡 Refreshing tree visualization...");
            await updateTreeVisualization(treeName, workbookName);
        } catch (error) {
            console.error("❌ Error:", error);
            alert("⚠️ Failed to modify tree.");
        }
    });
}

/** UPLOAD RAW CSV */
function attachUploadRawCsvListener() {
    const uploadRawCsvButton = document.getElementById("upload-raw-csv");
    if (!uploadRawCsvButton) return;

    uploadRawCsvButton.addEventListener("click", async () => {
        console.log("🚀 Upload Raw CSV button clicked!");

        const fileInput = document.getElementById("raw-csv");
        if (!fileInput || !fileInput.files.length) {
            alert("𛲝 Please select a CSV file to upload.");
            return;
        }

        const formData = new FormData();
        formData.append("file", fileInput.files[0]);

        try {
            console.log("📡 Sending CSV file to backend...");
            const response = await fetch("http://127.0.0.1:8000/duplicates/upload_raw_csv/", {
                method: "POST",
                body: formData,
            });

            if (!response.ok) {
                throw new Error(`❌ Upload failed with status: ${response.status}`);
            }

            const result = await response.json();
            console.log("✅ File uploaded successfully:", result);
            alert(result.message);

            // Show duplicates section after successful upload
            toggleVisibility("duplicates-section", true);
        } catch (error) {
            console.error("❌ Error uploading raw CSV:", error);
            alert("⚠️ Failed to upload raw CSV. Check the console for details.");
        }
    });
}

// Utility functions
// Function to populate the duplicates table dynamically
function populateDuplicatesTable(duplicatesData) {
    const tableHeader = document.getElementById("duplicates-table-header");
    const tableBody = document.getElementById("duplicates-table-body");

    // Clear existing table content
    tableHeader.innerHTML = "";
    tableBody.innerHTML = "";

    if (duplicatesData.length === 0) {
        alert("👏 No duplicates found!");
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

/** RESOLVE DUPLICATES */
function attachResolveDuplicatesListener() {
    const resolveDuplicatesButton = document.getElementById("resolve-duplicates");
    if (!resolveDuplicatesButton) return;

    resolveDuplicatesButton.addEventListener("click", async () => {
        console.log("🚀 Resolve Duplicates button clicked!");

        const groupColumn = document.getElementById("group-column").value.trim();
        const keyColumn = document.getElementById("key-column").value.trim();
        const valueColumn = document.getElementById("value-column").value.trim();

        if (!groupColumn || !keyColumn || !valueColumn) {
            alert("🧐 All fields are required.");
            return;
        }

        const rawCsvFile = document.getElementById("raw-csv").files[0];
        if (!rawCsvFile) {
            alert("⤴️ Please upload a raw CSV file before resolving duplicates.");
            return;
        }

        const formData = new FormData();
        formData.append("file", rawCsvFile);
        formData.append("group_column", groupColumn);
        formData.append("key_column", keyColumn);
        formData.append("value_column", valueColumn);

        try {
            console.log("📡 Sending request to resolve duplicates...");
            const response = await fetch("http://127.0.0.1:8000/duplicates/get_duplicates/", {
                method: "POST",
                body: formData,
            });

            if (!response.ok) {
                throw new Error(`❌ Fetching duplicates failed with status: ${response.status}`);
            }

            const result = await response.json();
            console.log("✅ Resolve Duplicates response:", result);

            if (result.duplicates && result.duplicates.length > 0) {
                alert(result.message);
                populateDuplicatesTable(result.duplicates);
            } else {
                alert("✅ No duplicates found.");
            }
        } catch (error) {
            console.error("❌ Error resolving duplicates:", error);
            alert("❌ Failed to resolve duplicates. Check the console for details.");
        }
    });
}

/** SUBMIT SELECTED ROWS */
function attachSubmitSelectedRowsListener() {
    const submitSelectedRowsButton = document.getElementById("submit-selected-rows");
    if (!submitSelectedRowsButton) return;

    submitSelectedRowsButton.addEventListener("click", async () => {
        console.log("🚀 Submit Selected Rows button clicked!");

        const selectedRows = Array.from(
            document.querySelectorAll("#duplicates-table-body input[type='checkbox']:checked")
        ).map((checkbox) => parseInt(checkbox.value));

        if (selectedRows.length === 0) {
            alert("💡 No rows selected. Keeping all rows.");
        }

        const groupColumn = document.getElementById("group-column").value.trim();
        const keyColumn = document.getElementById("key-column").value.trim();
        const valueColumn = document.getElementById("value-column").value.trim();
        const rawCsvFile = document.getElementById("raw-csv").files[0];

        if (!rawCsvFile || !groupColumn || !keyColumn || !valueColumn) {
            alert("⚠️ All fields are required, and a file must be uploaded.");
            return;
        }

        const formData = new FormData();
        formData.append("file", rawCsvFile);
        formData.append("group_column", groupColumn);
        formData.append("key_column", keyColumn);
        formData.append("value_column", valueColumn);
        formData.append("rows_to_remove", JSON.stringify(selectedRows));

        try {
            console.log("📡 Sending request to resolve duplicates...");
            const response = await fetch("http://127.0.0.1:8000/duplicates/resolve_duplicates/", {
                method: "POST",
                body: formData,
            });

            if (!response.ok) {
                throw new Error(`❌ Resolving duplicates failed with status: ${response.status}`);
            }

            const result = await response.json();
            console.log("✅ Submit Selected Rows response:", result);

            alert(result.message);

            // Ensure the parent paths section is populated
            await displayParentPathInputs();
        } catch (error) {
            console.error("❌ Error resolving duplicates:", error);
            alert("⚠️ Failed to resolve duplicates. Check the console for details.");
        }
    });
}

/** ATTACH SET PARENT PATHS LISTENER */
function attachSetParentPathsListener() {
    const setParentPathsButton = document.getElementById("set-parent-paths");
    if (!setParentPathsButton) {
        console.error("❌ Set Parent Paths button not found in DOM when attaching listener!");
        return;
    }

    console.log("✅ Set Parent Paths listener attached.");

    setParentPathsButton.addEventListener("click", async () => {
        console.log("🚀 Set Parent Paths button clicked!");

        const treeName = document.getElementById("tree-name").value.trim();
        const workbookName = document.getElementById("workbook-name").value.trim();

        // Alert the user but continue if missing
        // if (!treeName || !workbookName) {
        //     alert("⚠️ Warning: You have not entered a Tree Name or Workbook Name. The lookup may not push correctly.");
        // }

        try {
            setParentPathsButton.disabled = true; // Prevent multiple submissions

            const parentPaths = {};
            const parentPathsDiv = document.getElementById("parent-paths-section");
            const inputs = parentPathsDiv.querySelectorAll("input");

            inputs.forEach((input) => {
                const name = input.id.replace("parent-path-", "");
                parentPaths[name] = input.value.trim();
            });

            const groupColumn = document.getElementById("group-column").value.trim();
            const keyColumn = document.getElementById("key-column").value.trim();
            const valueColumn = document.getElementById("value-column").value.trim();

            if (!groupColumn || !keyColumn || !valueColumn || Object.keys(parentPaths).length === 0) {
                alert("❌ All fields and parent paths are required.");
                return;
            }

            const payload = {
                parent_paths: parentPaths,
                group_column: groupColumn,
                key_column: keyColumn,
                value_column: valueColumn,
            };

            console.log("📡 Sending request to set Parent Paths:", JSON.stringify(payload));

            const response = await fetch("http://127.0.0.1:8000/duplicates/set_parent_paths/", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify(payload),
            });

            if (!response.ok) {
                throw new Error(`❌ Failed to set Parent Paths: ${response.status}`);
            }

            const result = await response.json();
            console.log("✅ Set Parent Paths response:", result);

            alert(result.message);

            // Ensure "Push Lookup" section is visible
            const pushLookupSection = document.getElementById("push-lookup-section");
            if (pushLookupSection) {
                pushLookupSection.style.display = "block";
                console.log("✅ 'Push Lookup' section is now visible.");
            } else {
                console.error("❌ 'Push Lookup' section not found in the DOM.");
            }

        } catch (error) {
            console.error("❌ Error setting Parent Paths:", error);
            alert("An error occurred while setting Parent Paths. Check the console for details.");
        } finally {
            setParentPathsButton.disabled = false;
        }
    });
}

document.getElementById("push-lookup-btn").addEventListener("click", async () => {
    const treeNameInput = document.querySelector("#lookup-controls #tree-name");
    const workbookNameInput = document.querySelector("#lookup-controls #workbook-name");

    const treeName = treeNameInput ? treeNameInput.value.trim() : "";
    const workbookName = workbookNameInput ? workbookNameInput.value.trim() : "";

    if (!treeName || !workbookName) {
        alert("⚠️ Warning: You have not entered a Tree Name or Workbook Name. The lookup may not push correctly.");
        return;
    }

    try {
        console.log(`📡 Sending lookup push request for Tree: ${treeName}, Workbook: ${workbookName}`);

        const response = await fetch("http://127.0.0.1:8000/duplicates/push_lookup/", {
            method: "POST",
            headers: { "Content-Type": "application/x-www-form-urlencoded" },
            body: new URLSearchParams({ tree_name: treeName, workbook_name: workbookName }),
        });

        if (!response.ok) {
            throw new Error(`Push failed with status: ${response.status}`);
        }

        const result = await response.json();
        console.log("✅ Lookup Push Response:", result);
        alert(result.message);

        // ✅ Call visualization update after successful push
        updateTreeVisualization(treeName, workbookName);

    } catch (error) {
        console.error("Error pushing lookup data:", error);
        alert("Failed to push lookup data. Check the console for details.");
    }
});

/** APPLY USER-SPECIFIC SETTINGS */
function attachApplyUserSpecificListener() {
    const applyUserSpecificButton = document.getElementById("apply-user-specific");
    if (!applyUserSpecificButton) return;

    applyUserSpecificButton.addEventListener("click", async () => {
        console.log("🚀 Apply User-Specific Settings button clicked!");

        const rowsInput = document.getElementById("rows-to-keep-input").value.trim();
        const rowsToKeep = rowsInput
            .split(",")
            .map((index) => parseInt(index.trim(), 10))
            .filter((index) => !isNaN(index));

        console.log("✅ Rows to keep:", rowsToKeep);

        if (rowsToKeep.length === 0) {
            alert("No rows selected. Keeping all rows.");
        }

        // Fetch lookup names if needed
        const lookupNames = await fetchLookupNames();
        console.log("✅ Lookup Names Fetched:", lookupNames);

        if (lookupNames.length > 0) {
            // If lookup names exist, display parent path input fields
            await displayParentPathInputs();
        }
    });
}

/** SETUP CONTROL TOGGLE FOR WORKFLOWS */
function setupControlToggle() {
    const controlSelector = document.getElementById("control-selector");
    const assetTreeControls = document.getElementById("asset-tree-controls");
    const lookupControls = document.getElementById("lookup-controls");

    if (!controlSelector || !assetTreeControls || !lookupControls) {
        console.error("❌ Dropdown or control sections are missing in the DOM!");
        return;
    }

    console.log("✅ Setting up control toggle listener.");
    controlSelector.addEventListener("change", (event) => {
        const selectedValue = event.target.value;
        console.log("📌 Dropdown value changed to:", selectedValue);

        // Toggle visibility of sections based on dropdown selection
        assetTreeControls.style.display = selectedValue === "asset-tree-controls" ? "block" : "none";
        lookupControls.style.display = selectedValue === "lookup-controls" ? "block" : "none";

        console.log(`✅ Controls toggled to: ${selectedValue}`);
    });

    // Set initial visibility
    assetTreeControls.style.display = "block";
    lookupControls.style.display = "none";
    console.log("✅ Initial visibility: Asset Tree Controls shown, Lookup Workflow hidden.");
}

/** Setup Dropdown Visibility */
function setupDropdownVisibility() {
    const controlSelector = document.getElementById("control-selector");
    const assetTreeControls = document.getElementById("asset-tree-controls");
    const lookupControls = document.getElementById("lookup-controls");

    if (!controlSelector || !assetTreeControls || !lookupControls) {
        console.error("Dropdown or control sections are missing in the DOM.");
        return;
    }

    console.log("📌 Setting up dropdown listener.");
    controlSelector.addEventListener("change", (event) => {
        console.log("Dropdown value changed to:", event.target.value);
        assetTreeControls.style.display = event.target.value === "asset-tree-controls" ? "block" : "none";
        lookupControls.style.display = event.target.value === "lookup-controls" ? "block" : "none";
        console.log(`✅ Switched to: ${event.target.value}`);
    });
}

/** 🔹 Setup Initial Visibility */
function setupInitialVisibility() {
    const assetTreeControls = document.getElementById("asset-tree-controls");
    const lookupControls = document.getElementById("lookup-controls");

    if (assetTreeControls && lookupControls) {
        assetTreeControls.style.display = "block";
        lookupControls.style.display = "none";
        console.log("✅ Initial visibility: Asset Tree Controls shown, Lookup Workflow hidden.");
    } else {
        console.error("❌ Control sections not found in the DOM.");
    }
}

// Declare `isListenersAttached` globally before using it
let isListenersAttached = false;  // Track event listener attachment

/** Attach All Event Listeners */
function attachEventListeners() {
    const listeners = [
        attachCreateTreeListener,
        attachCsvUploadListener,
        attachProcessCsvListener,
        attachClearTreeListener,
        attachGenerateLookupListener, 
        attachSearchTreeListener,
        attachInsertItemListener,
        attachModifyTreeListener,
        attachUploadRawCsvListener,
        attachResolveDuplicatesListener,
        attachSubmitSelectedRowsListener,
        attachSetParentPathsListener,
        attachApplyUserSpecificListener,
        setupControlToggle,
        setupDropdownVisibility,
        setupInitialVisibility
    ];

    listeners.forEach((listener) => {
        if (typeof listener === "function") {
            listener(); // Only call it if the function exists
        } else {
            console.warn(`⚠️ Skipping undefined listener: ${listener.name || "Unknown function"}`);
        }
    });
}

document.addEventListener("DOMContentLoaded", function () {
    if (!isListenersAttached) {
        console.log("🚀 DOM fully loaded, attaching event listeners...");
        attachEventListeners();
        isListenersAttached = true;
    }
});