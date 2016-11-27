function checkBannerHrefs() {
    $(document).ready(function () {
        $('a[href="#"]').each(function () {
            if ($(this).closest("[class*='banner']")) {
                $(this).removeAttr("href");
            }
        });
    })
}