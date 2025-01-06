// Handle CSV Upload
document.getElementById("upload-csv").addEventListener("click", async (event) => {
    event.preventDefault(); // Prevent the default form submission
    const fileInput = document.getElementById("csv-file");
    const formData = new FormData(); // Use FormData to handle multipart form data
    formData.append("file", fileInput.files[0]);

    try {
        const response = await fetch("http://127.0.0.1:8000/upload_csv/", {
            method: "POST",
            body: formData,
        });

        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }

        const result = await response.json();
        console.log("File uploaded successfully:", result);
        alert(`File uploaded: ${result.filename}`);
    } catch (error) {
        console.error("Error uploading file:", error);
        alert("Error uploading file. Check the console for details.");
    }
});

// Process CSV and update the visualization
document.getElementById("process-csv").addEventListener("click", async () => {
    console.log("Process CSV button clicked."); // Debug log to confirm the button was clicked
    
    try {
        const response = await fetch("http://127.0.0.1:8000/process_csv/", {
            method: "POST",
        });

        if (!response.ok) {
            throw new Error(`Processing failed with status: ${response.status}`);
        }

        const result = await response.json();
        console.log("CSV processed response:", result);

        // Update the UI with the result
        const visualizationDiv = document.getElementById("tree-visualization");
        if (result.tree_structure) {
            visualizationDiv.innerHTML = `<pre style="white-space: pre-wrap;">${result.tree_structure}</pre>`;
        } else {
            visualizationDiv.innerHTML = `<p class="placeholder-message">Tree visualization is not available. Please ensure the tree is built and processed correctly.</p>`;
        }
    } catch (error) {
        console.error("Error processing CSV:", error);
        alert("Failed to process the CSV. Check the console for details.");
    }
});

// Visualize Tree
document.getElementById("visualize-tree").addEventListener("click", async () => {
    console.log("Visualize Tree button clicked."); // Debug log

    try {
        const response = await fetch("http://127.0.0.1:8000/visualize_tree/", {
            method: "GET",
        });

        if (!response.ok) {
            throw new Error(`Visualization failed with status: ${response.status}`);
        }

        const result = await response.json();
        console.log("Tree visualization response:", result);

        // Update the tree visualization
        const visualizationDiv = document.getElementById("tree-visualization");
        visualizationDiv.innerHTML = `<pre>${JSON.stringify(result.tree_structure, null, 2)}</pre>`;
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

// Push Tree
document.getElementById("push-tree").addEventListener("click", async () => {
    console.log("Push Tree button clicked."); // Debug log
    
    try {
        const response = await fetch("http://127.0.0.1:8000/push_tree/", {
            method: "POST",
            headers: { "Content-Type": "application/x-www-form-urlencoded" },
            body: new URLSearchParams({ tree_name: "Asset Tree" }),
        });

        if (!response.ok) {
            throw new Error(`Push failed with status: ${response.status}`);
        }

        const result = await response.json();
        alert(result.message);
    } catch (error) {
        console.error("Error pushing tree:", error);
        alert("Failed to push the tree. Check the console for details.");
    }
});

// Handle Create Empty Tree
document.getElementById("create-empty-tree").addEventListener("click", async () => {
    console.log("Attaching event listener to 'Create Empty Tree' button."); // Debug log
    const treeNameInput = document.getElementById("tree-name");
    const treeName = treeNameInput.value.trim();

    if (!treeName) {
        alert("Please enter a tree name.");
        return;
    }

    try {
        const response = await fetch("http://127.0.0.1:8000/create_empty_tree/", {
            method: "POST",
            headers: {
                "Content-Type": "application/json",
            },
            body: JSON.stringify({ tree_name: treeName }),
        });

        if (!response.ok) {
            throw new Error(`Failed to create tree: ${response.status}`);
        }

        const result = await response.json();
        console.log("Empty tree created response:", result);

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
    const treeName = document.getElementById("tree-name").value;
    if (!treeName) {
        alert("Please provide a tree name.");
        return;
    }
    try {
        const response = await fetch(`http://127.0.0.1:8000/search_tree/?tree_name=${treeName}`, {
            method: "GET",
        });

        const result = await response.json();
        console.log("Tree search response:", result);

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