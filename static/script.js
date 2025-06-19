document.addEventListener("DOMContentLoaded", () => {
  // ... (kode event listener dan fetch tetap sama)
  const recommendForm = document.getElementById("recommendForm");
  const submitBtn = document.getElementById("submitBtn");
  const resultSection = document.getElementById("resultSection");
  const recommendationsDiv = document.getElementById("recommendations");
  const errorSection = document.getElementById("errorSection");
  const loader = document.getElementById("loader");

  recommendForm.addEventListener("submit", async (event) => {
    event.preventDefault();
    const query = document.getElementById("query").value;
    const topN = parseInt(document.getElementById("top_n").value, 10);
    submitBtn.disabled = true;
    submitBtn.innerHTML =
      '<span class="spinner-border spinner-border-sm" role="status" aria-hidden="true"></span> Loading...';
    errorSection.style.display = "none";
    recommendationsDiv.innerHTML = "";
    resultSection.style.display = "block";
    loader.style.display = "block";

    try {
      const response = await fetch("/recommend/query", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ query: query, top_n: topN }),
      });
      if (!response.ok) {
        const errorData = await response.json();
        throw new Error(
          errorData.error || `HTTP error! Status: ${response.status}`
        );
      }
      const data = await response.json();
      displayRecommendations(data.recommendations);
    } catch (error) {
      console.error("Error fetching recommendations:", error);
      displayError(error.message);
    } finally {
      loader.style.display = "none";
      submitBtn.disabled = false;
      submitBtn.innerHTML = "Get Recommendations";
    }
  });

  // ▼▼▼ FUNGSI INI DIUBAH TOTAL UNTUK MENAMPILKAN LEBIH BANYAK DATA ▼▼▼
  function displayRecommendations(recommendations) {
    if (!recommendations || recommendations.length === 0) {
      recommendationsDiv.innerHTML =
        '<p class="text-center text-muted">No job recommendations found for your query. Please try different keywords.</p>';
      return;
    }

    const recommendationsHTML = recommendations
      .map((job, index) => {
        // Helper untuk membuat badge jika data ada
        const createBadge = (value, className = "bg-secondary") => {
          return value
            ? `<span class="badge ${className}">${value}</span>`
            : "";
        };

        const flagHTML = job.flag_url
          ? `<img src="${job.flag_url}" alt="${job.country} Flag" class="country-flag" title="${job.country}">`
          : "";

        // ID unik untuk setiap elemen collapse
        const collapseId = `details-${index}`;

        return `
          <div class="card recommendation-card mb-3">
            <div class="card-header card-header-flex">
              <h5 class="card-title mb-0">${job.title}</h5>
              ${flagHTML}
            </div>
            <div class="card-body">
              <p class="card-text mb-2"><span class="job-detail-label">Company:</span> ${
                job.company
              }</p>
              <p class="card-text mb-2"><span class="job-detail-label">Location:</span> ${
                job.location
              }, ${job.country}</p>
              <p class="card-text"><span class="job-detail-label">Experience:</span> ${
                job.experience
              }</p>
              
              <!-- Meta data menggunakan Badge -->
              <div class="job-meta-badges">
                ${createBadge(job.work_type, "bg-info text-dark")}
                ${createBadge(job.salary, "bg-success")}
                ${createBadge(job.preference, "bg-warning text-dark")}
              </div>

              <!-- Tombol untuk menampilkan/menyembunyikan detail -->
              <button class="btn btn-outline-primary btn-sm mt-2" type="button" data-bs-toggle="collapse" data-bs-target="#${collapseId}" aria-expanded="false" aria-controls="${collapseId}">
                Show Details
              </button>

              <!-- Konten yang bisa di-collapse -->
              <div class="collapse mt-3" id="${collapseId}">
                <div class="details-collapse">
                  <h6>Key Skills</h6>
                  <p>${job.skills || "Not specified"}</p>
                  
                  <h6>Job Description</h6>
                  <p>${job.desc || "Not specified"}</p>
                  
                  <h6>Qualifications</h6>
                  <p>${job.qualification || "Not specified"}</p>
                  
                  <h6>Contact</h6>
                  <p>Reach out to <strong>${
                    job.contact_person || "HR"
                  }</strong> at <em>${job.contact || "Not provided"}</em></p>
                </div>
              </div>
            </div>
            <div class="card-footer text-muted">
              <span class="badge bg-primary rounded-pill">Similarity: ${
                job.similarity_score
              }</span>
            </div>
          </div>
        `;
      })
      .join("");

    recommendationsDiv.innerHTML = recommendationsHTML;
  }

  function displayError(message) {
    errorSection.textContent = `An error occurred: ${message}`;
    errorSection.style.display = "block";
    resultSection.style.display = "none";
  }
});
