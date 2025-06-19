document.addEventListener("DOMContentLoaded", () => {
  // === 1. PENGAMBILAN ELEMEN DOM ===
  // Mengambil semua elemen interaktif dari halaman HTML
  const recommendForm = document.getElementById("recommendForm");
  const submitBtn = document.getElementById("submitBtn");
  const resultSection = document.getElementById("resultSection");
  const recommendationsDiv = document.getElementById("recommendations");
  const errorSection = document.getElementById("errorSection");
  const loader = document.getElementById("loader");
  const countryFilterContainer = document.getElementById(
    "countryFilterContainer"
  );

  // === 2. STATE MANAGEMENT ===
  // Variabel untuk menyimpan hasil penuh dari API agar bisa difilter tanpa request ulang
  let fullRecommendations = [];

  // === 3. EVENT LISTENER UTAMA (SUBMIT FORM) ===
  recommendForm.addEventListener("submit", async (event) => {
    event.preventDefault(); // Mencegah form dari me-reload halaman

    // --- Mengatur UI ke state loading ---
    submitBtn.disabled = true;
    submitBtn.innerHTML =
      '<span class="spinner-border spinner-border-sm" role="status" aria-hidden="true"></span> Loading...';
    errorSection.style.display = "none";
    recommendationsDiv.innerHTML = ""; // Kosongkan hasil sebelumnya
    countryFilterContainer.innerHTML = ""; // Kosongkan filter sebelumnya
    resultSection.style.display = "block"; // Tampilkan section hasil
    loader.style.display = "block"; // Tampilkan loader

    try {
      // --- Mengambil input dan mengirim request ke backend ---
      const query = document.getElementById("query").value;
      const topN = parseInt(document.getElementById("top_n").value, 10);

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

      // --- Memproses data yang diterima ---
      fullRecommendations = data.recommendations; // Simpan semua hasil ke state
      displayCountryFilters(fullRecommendations); // Buat tombol-tombol filter
      displayRecommendations(fullRecommendations); // Tampilkan semua hasil awal
    } catch (error) {
      console.error("Error fetching recommendations:", error);
      displayError(error.message);
    } finally {
      // --- Mengembalikan UI ke state normal setelah selesai ---
      loader.style.display = "none";
      submitBtn.disabled = false;
      submitBtn.innerHTML = "Get Recommendations";
    }
  });

  // === 4. FUNGSI UNTUK MEMBUAT TOMBOL FILTER NEGARA ===
  function displayCountryFilters(recommendations) {
    if (recommendations.length === 0) {
      countryFilterContainer.innerHTML = ""; // Kosongkan jika tidak ada hasil
      return;
    }

    // Dapatkan daftar negara unik, buang nilai kosong (null/undefined), lalu urutkan
    const countries = [
      ...new Set(recommendations.map((job) => job.country).filter(Boolean)),
    ].sort();

    // Jika hanya ada 1 negara (atau tidak ada), tidak perlu menampilkan filter
    if (countries.length <= 1) {
      countryFilterContainer.innerHTML = "";
      return;
    }

    // Buat HTML untuk tombol-tombol filter
    let filtersHTML =
      '<button class="btn btn-outline-secondary filter-btn active" data-country="all">All Countries</button>';
    countries.forEach((country) => {
      filtersHTML += `<button class="btn btn-outline-secondary filter-btn" data-country="${country}">${country}</button>`;
    });

    countryFilterContainer.innerHTML = `<span class="fw-bold me-2">Filter by Country:</span> ${filtersHTML}`;
  }

  // === 5. FUNGSI UNTUK MENAMPILKAN KARTU PEKERJAAN ===
  function displayRecommendations(jobsToDisplay) {
    // Tampilkan pesan jika tidak ada pekerjaan yang cocok dengan filter saat ini
    if (!jobsToDisplay || jobsToDisplay.length === 0) {
      recommendationsDiv.innerHTML =
        '<p class="text-center text-muted">No jobs match the current filter.</p>';
      return;
    }

    const recommendationsHTML = jobsToDisplay
      .map((job, index) => {
        // Helper untuk membuat badge Bootstrap jika data ada
        const createBadge = (value, className = "bg-secondary") => {
          return value
            ? `<span class="badge ${className}">${value}</span>`
            : "";
        };

        // Tentukan ikon yang akan ditampilkan (bendera atau globe)
        const iconTitle = job.country || "Global";
        const iconAltText = `${iconTitle} location icon`;
        const flagHTML = `<img src="${job.flag_url}" alt="${iconAltText}" class="country-flag" title="${iconTitle}">`;

        // Buat ID unik untuk setiap elemen collapse agar tidak bentrok
        const collapseId = `details-${job.job_id}-${index}`;
        const mapId = `map-${job.job_id}-${index}`;

        // Template literal untuk membuat HTML kartu secara dinamis
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
            }, ${job.country || "N/A"}</p>
            <p class="card-text"><span class="job-detail-label">Experience:</span> ${
              job.experience
            }</p>
            
            <div class="job-meta-badges">
              ${createBadge(job.work_type, "bg-info text-dark")}
              ${createBadge(job.salary, "bg-success")}
              ${createBadge(job.preference, "bg-warning text-dark")}
            </div>

            <button class="btn btn-outline-primary btn-sm mt-2" type="button" data-bs-toggle="collapse" data-bs-target="#${collapseId}" aria-expanded="false" aria-controls="${collapseId}">
              Show Details
            </button>

            <div class="collapse mt-3" id="${collapseId}">
              <div class="details-collapse p-3">
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
                <!-- PETA -->
                ${
                  job.latitude && job.longitude
                    ? `<div class="job-map" id="${mapId}"></div><a href="https://maps.google.com/?q=${job.latitude},${job.longitude}" class="btn btn-sm btn-outline-success mt-2" target="_blank" rel="noopener">Lihat di Google Maps</a>`
                    : ""
                }
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

    // Inisialisasi peta Leaflet saat collapse dibuka
    jobsToDisplay.forEach((job, index) => {
      if (job.latitude && job.longitude) {
        const collapseId = `details-${job.job_id}-${index}`;
        const mapId = `map-${job.job_id}-${index}`;
        const collapseEl = document.getElementById(collapseId);
        if (collapseEl) {
          collapseEl.addEventListener("show.bs.collapse", function () {
            // Cek jika peta sudah pernah diinisialisasi
            if (!collapseEl.dataset.mapInitialized) {
              setTimeout(() => {
                const mapDiv = document.getElementById(mapId);
                if (mapDiv && !mapDiv.dataset.mapLoaded) {
                  const map = L.map(mapId, {
                    center: [job.latitude, job.longitude],
                    zoom: 4,
                    scrollWheelZoom: false,
                    zoomControl: true,
                  });
                  L.tileLayer(
                    "https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png",
                    {
                      attribution:
                        '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors',
                    }
                  ).addTo(map);
                  L.marker([job.latitude, job.longitude]).addTo(map);
                  mapDiv.dataset.mapLoaded = "1";
                }
              }, 250); // Delay agar div sudah visible
              collapseEl.dataset.mapInitialized = "1";
            }
          });
        }
      }
    });
  }

  // === 6. EVENT LISTENER UNTUK FILTER (EVENT DELEGATION) ===
  countryFilterContainer.addEventListener("click", (event) => {
    // Cek apakah yang diklik adalah sebuah tombol filter
    if (event.target && event.target.matches(".filter-btn")) {
      const selectedCountry = event.target.dataset.country;

      // Update tampilan tombol: hapus 'active' dari semua, lalu tambahkan ke yang diklik
      countryFilterContainer
        .querySelectorAll(".filter-btn")
        .forEach((btn) => btn.classList.remove("active"));
      event.target.classList.add("active");

      // Terapkan filter
      if (selectedCountry === "all") {
        displayRecommendations(fullRecommendations); // Tampilkan semua
      } else {
        const filteredJobs = fullRecommendations.filter(
          (job) => job.country === selectedCountry
        );
        displayRecommendations(filteredJobs); // Tampilkan hasil yang sudah difilter
      }
    }
  });

  // === 7. FUNGSI UNTUK MENAMPILKAN ERROR ===
  function displayError(message) {
    errorSection.textContent = `An error occurred: ${message}`;
    errorSection.style.display = "block";
    resultSection.style.display = "none";
  }

  // === DARK MODE TOGGLE ===
  const darkModeToggle = document.getElementById("darkModeToggle");
  const darkModeIcon = document.getElementById("darkModeIcon");

  function setDarkMode(enabled) {
    if (enabled) {
      document.body.classList.add("dark-mode");
      darkModeIcon.textContent = "☀️";
      localStorage.setItem("darkMode", "1");
    } else {
      document.body.classList.remove("dark-mode");
      darkModeIcon.textContent = "🌙";
      localStorage.setItem("darkMode", "0");
    }
  }

  // Inisialisasi dari localStorage
  const darkPref = localStorage.getItem("darkMode");
  setDarkMode(darkPref === "1");

  darkModeToggle.addEventListener("click", () => {
    const isDark = document.body.classList.contains("dark-mode");
    setDarkMode(!isDark);
  });
});
